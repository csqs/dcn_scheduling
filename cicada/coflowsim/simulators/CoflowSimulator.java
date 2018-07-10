package coflowsim.simulators;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

import coflowsim.datastructures.Flow;
import coflowsim.datastructures.Job;
import coflowsim.datastructures.ReduceTask;
import coflowsim.datastructures.Task;
import coflowsim.datastructures.Task.TaskType;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;
import coflowsim.utils.Constants.SHARING_ALGO;
import coflowsim.utils.Utils;

//csqs
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Random;

/**
 * Implements {@link coflowsim.simulators.Simulator} for coflow-level scheduling policies (FIFO,
 * SCF, NCF, LCF, and SEBF).
 */
public class CoflowSimulator extends Simulator {

  Vector<Job> sortedJobs;

  double[] sendBpsFree;
  double[] recvBpsFree;

  /**
   * {@inheritDoc}
   */
  public CoflowSimulator(
      SHARING_ALGO sharingAlgo,
      TraceProducer traceProducer,
      boolean offline,
      boolean considerDeadline,
      double deadlineMultRandomFactor) {

    super(sharingAlgo, traceProducer, offline, considerDeadline, deadlineMultRandomFactor);
    assert (sharingAlgo == SHARING_ALGO.FIFO || sharingAlgo == SHARING_ALGO.SCF
        || sharingAlgo == SHARING_ALGO.NCF || sharingAlgo == SHARING_ALGO.LCF
        || sharingAlgo == SHARING_ALGO.SEBF);
  }

  /** {@inheritDoc} */
  @Override
  protected void initialize(TraceProducer traceProducer) {
    super.initialize(traceProducer);

    this.sendBpsFree = new double[NUM_RACKS];
    this.recvBpsFree = new double[NUM_RACKS];
    resetSendRecvBpsFree();

    this.sortedJobs = new Vector<Job>();
  }

  protected void resetSendRecvBpsFree() {
    Arrays.fill(this.sendBpsFree, Constants.RACK_BITS_PER_SEC);
    Arrays.fill(this.recvBpsFree, Constants.RACK_BITS_PER_SEC);
  }

  /**
   * Admission control for the deadline-sensitive case.
   */
  @Override
  protected boolean admitThisJob(Job j) {
    if (considerDeadline) {
      updateRatesDynamicAlpha(Constants.VALUE_UNKNOWN, true);
      double currentAlpha = calcAlphaOnline(j, sendBpsFree, recvBpsFree);
      if (currentAlpha == Constants.VALUE_UNKNOWN || currentAlpha > j.deadlineDuration / 1000.0)
        return false;
    }
    return true;
  }

  /** {@inheritDoc} */
  @Override
  protected void uponJobAdmission(Job j) {
    for (Task t : j.tasks) {
      if (t.taskType == TaskType.REDUCER) {
        ReduceTask rt = (ReduceTask) t;

        // Update start stats for the task and its parent job
        rt.startTask(CURRENT_TIME);

        // Add the parent job to the collection of active jobs
        if (!activeJobs.containsKey(rt.parentJob.jobName)) {
          activeJobs.put(rt.parentJob.jobName, rt.parentJob);
          addToSortedJobs(j);
        }
        incNumActiveTasks();
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void afterJobAdmission(long curTime) {
    layoutFlowsInJobOrder();
    updateRatesDynamicAlpha(curTime, false);
  }

  /** {@inheritDoc} */
  @Override
  protected void onSchedule(long curTime) {
    proceedFlowsInAllRacks(curTime, Constants.SIMULATION_QUANTA);
  }

  /** {@inheritDoc} */
  @Override
  protected void afterJobDeparture(long curTime) {
    updateRatesDynamicAlpha(curTime, false);
  }

  /**
   * Calculate dynamic alpha (in seconds)
   * 
   * @param job
   *          coflow to calculate for
   * @param sFree
   *          free bps in uplinks
   * @param rFree
   *          free bps in downlinks
   * @return
   */
  private double calcAlphaOnline(Job job, double[] sFree, double[] rFree) {
    double[] sendBytes = new double[NUM_RACKS];
    Arrays.fill(sendBytes, 0.0);
    double[] recvBytes = new double[NUM_RACKS];
    Arrays.fill(recvBytes, 0.0);

    // Calculate based on bytes remaining
    for (Task t : job.tasks) {
      if (t.taskType == TaskType.REDUCER) {
        ReduceTask rt = (ReduceTask) t;
        recvBytes[rt.taskID] = rt.shuffleBytesLeft;
        for (Flow f : rt.flows) {
          sendBytes[f.mapper.taskID] += f.bytesRemaining;
        }
      }
    }

    // Scale by available capacity
    for (int i = 0; i < NUM_RACKS; i++) {
      if ((sendBytes[i] > 0 && sFree[i] <= Constants.ZERO)
          || (recvBytes[i] > 0 && rFree[i] <= Constants.ZERO)) {
        return Constants.VALUE_UNKNOWN;
      }

      sendBytes[i] = sendBytes[i] * 8 / sFree[i];
      recvBytes[i] = recvBytes[i] * 8 / rFree[i];
    }

    return Math.max(Utils.max(sendBytes), Utils.max(recvBytes));
  }

  /**
   * Update rates of flows for each coflow using a dynamic alpha, calculated based on remaining
   * bytes of the coflow and current condition of the network
   * 
   * @param curTime
   *          current time
   * @param trialRun
   *          if true, do NOT change any jobs allocations
   */
  private void updateRatesDynamicAlpha(final long curTime, boolean trialRun) {
    // Reset sendBpsFree and recvBpsFree
    resetSendRecvBpsFree();

    // For keeping track of jobs with invalid currentAlpha
    Vector<Job> skippedJobs = new Vector<Job>();

    // Recalculate rates
    for (Job sj : sortedJobs) {
      // Reset ALL rates first
      for (Task t : sj.tasks) {
        if (t.taskType == TaskType.REDUCER) {
          ReduceTask rt = (ReduceTask) t;
          for (Flow f : rt.flows) {
            f.currentBps = 0;
          }
        }
      }

      double[] sendUsed = new double[NUM_RACKS];
      double[] recvUsed = new double[NUM_RACKS];
      Arrays.fill(sendUsed, 0.0);
      Arrays.fill(recvUsed, 0.0);

      double currentAlpha = calcAlphaOnline(sj, sendBpsFree, recvBpsFree);
      if (currentAlpha == Constants.VALUE_UNKNOWN) {
        skippedJobs.add(sj);
        continue;
      }
      // Use deadline instead of alpha when considering deadlines
      if (considerDeadline) {
        currentAlpha = sj.deadlineDuration / 1000;
      }

      updateRatesDynamicAlphaOneJob(sj, currentAlpha, sendUsed, recvUsed, trialRun);

      // Remove capacity from ALL sources and destination for the entire job
      for (int i = 0; i < NUM_RACKS; i++) {
        sendBpsFree[i] -= sendUsed[i];
        recvBpsFree[i] -= recvUsed[i];
      }
    }

    // Work conservation
    if (!trialRun) {
      // First, consider all skipped ones and divide remaining bandwidth between their flows
      double[] sendUsed = new double[NUM_RACKS];
      double[] recvUsed = new double[NUM_RACKS];
      Arrays.fill(sendUsed, 0.0);
      Arrays.fill(recvUsed, 0.0);

      updateRatesFairShare(skippedJobs, sendUsed, recvUsed);

      // Remove capacity from ALL sources and destination for the entire job
      for (int i = 0; i < NUM_RACKS; i++) {
        sendBpsFree[i] -= sendUsed[i];
        recvBpsFree[i] -= recvUsed[i];
      }

      // Heuristic: Sort coflows by EDF and then refill
      // If there is no deadline, this simply sorts them by arrival time
      Vector<Job> sortedByEDF = new Vector<Job>(sortedJobs);
      Collections.sort(sortedByEDF, new Comparator<Job>() {
        public int compare(Job o1, Job o2) {
          int timeLeft1 = (int) (o1.simulatedStartTime + o1.deadlineDuration - curTime);
          int timeLeft2 = (int) (o2.simulatedStartTime + o2.deadlineDuration - curTime);
          return timeLeft1 - timeLeft2;
        }
      });

      for (Job sj : sortedByEDF) {
        for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) {
            continue;
          }

          ReduceTask rt = (ReduceTask) t;
          int dst = rt.taskID;
          if (recvBpsFree[dst] <= Constants.ZERO) {
            continue;
          }

          for (Flow f : rt.flows) {
            int src = f.mapper.taskID;

            double minFree = Math.min(sendBpsFree[src], recvBpsFree[dst]);
            if (minFree <= Constants.ZERO) minFree = 0.0;

            f.currentBps += minFree;
            sendBpsFree[src] -= minFree;
            recvBpsFree[dst] -= minFree;
          }
        }
      }
    }
  }

  /**
   * Update rates of individual flows from a collection of coflows while considering them as a
   * single coflow.
   * 
   * Modifies sendBpsFree and recvBpsFree
   * 
   * @param jobsToConsider
   *          Collection of {@link coflowsim.datastructures.Job} under consideration.
   * @param sendUsed
   *          initialized to zeros.
   * @param recvUsed
   *          initialized to zeros.
   * @return
   */
  private double updateRatesFairShare(
      Vector<Job> jobsToConsider,
      double[] sendUsed,
      double[] recvUsed) {
    double totalAlloc = 0.0;

    // Calculate the number of mappers and reducers in each port
    int[] numMapSideFlows = new int[NUM_RACKS];
    Arrays.fill(numMapSideFlows, 0);
    int[] numReduceSideFlows = new int[NUM_RACKS];
    Arrays.fill(numReduceSideFlows, 0);

    for (Job sj : jobsToConsider) {
      for (Task t : sj.tasks) {
        if (t.taskType != TaskType.REDUCER) {
          continue;
        }

        ReduceTask rt = (ReduceTask) t;
        int dst = rt.taskID;
        if (recvBpsFree[dst] <= Constants.ZERO) {
          continue;
        }

        for (Flow f : rt.flows) {
          int src = f.mapper.taskID;
          if (sendBpsFree[src] <= Constants.ZERO) {
            continue;
          }

          numMapSideFlows[src]++;
          numReduceSideFlows[dst]++;
        }
      }
    }

    for (Job sj : jobsToConsider) {
      for (Task t : sj.tasks) {
        if (t.taskType != TaskType.REDUCER) {
          continue;
        }

        ReduceTask rt = (ReduceTask) t;
        int dst = rt.taskID;
        if (recvBpsFree[dst] <= Constants.ZERO || numReduceSideFlows[dst] == 0) {
          continue;
        }

        for (Flow f : rt.flows) {
          int src = f.mapper.taskID;
          if (sendBpsFree[src] <= Constants.ZERO || numMapSideFlows[src] == 0) {
            continue;
          }

          // Determine rate based only on this job and available bandwidth
          double minFree = Math.min(sendBpsFree[src] / numMapSideFlows[src],
              recvBpsFree[dst] / numReduceSideFlows[dst]);
          if (minFree <= Constants.ZERO) {
            minFree = 0.0;
          }

          f.currentBps = minFree;

          // Remember how much capacity was allocated
          sendUsed[src] += f.currentBps;
          recvUsed[dst] += f.currentBps;
          totalAlloc += f.currentBps;
        }
      }
    }

    return totalAlloc;
  }

  /**
   * Update rates of individual flows of a given coflow.
   * 
   * @param sj
   *          {@link coflowsim.datastructures.Job} under consideration.
   * @param currentAlpha
   *          dynamic alpha is in Seconds (Normally, alpha is in Bytes).
   * @param sendUsed
   *          initialized to zeros.
   * @param recvUsed
   *          initialized to zeros.
   * @param trialRun
   *          if true, do NOT change any jobs allocations.
   * @return
   */
  private double updateRatesDynamicAlphaOneJob(
      Job sj,
      double currentAlpha,
      double[] sendUsed,
      double[] recvUsed,
      boolean trialRun) {

    double jobAlloc = 0.0;

    for (Task t : sj.tasks) {
      if (t.taskType != TaskType.REDUCER) {
        continue;
      }

      ReduceTask rt = (ReduceTask) t;
      int dst = rt.taskID;
      if (recvBpsFree[dst] <= Constants.ZERO) {
        continue;
      }

      for (Flow f : rt.flows) {
        int src = f.mapper.taskID;

        double curBps = f.bytesRemaining * 8 / currentAlpha;
        if (curBps > sendBpsFree[src] || curBps > recvBpsFree[dst]) {
          curBps = Math.min(sendBpsFree[src], recvBpsFree[dst]);
        }

        // Remember how much capacity was allocated
        sendUsed[src] += curBps;
        recvUsed[dst] += curBps;
        jobAlloc += curBps;

        // Update f.currentBps if required
        if (!trialRun) {
          f.currentBps = curBps;
        }
      }
    }

    return jobAlloc;
  }

  protected void addToSortedJobs(Job j) {
    if (considerDeadline) {
      sortedJobs.add(j);
      return;
    }

    if (sharingAlgo == SHARING_ALGO.FIFO) {
      sortedJobs.add(j);
    } else {
      int index = 0;
      for (Job sj : sortedJobs) {
        if (sharingAlgo == SHARING_ALGO.SCF && SCFComparator.compare(j, sj) < 0) {
          break;
        } else if (sharingAlgo == SHARING_ALGO.NCF && NCFComparator.compare(j, sj) < 0) {
          break;
        } else if (sharingAlgo == SHARING_ALGO.LCF && LCFComparator.compare(j, sj) < 0) {
          break;
        } else if (sharingAlgo == SHARING_ALGO.SEBF && SEBFComparator.compare(j, sj) < 0) {
          break;
        }
        index++;
      }
      sortedJobs.insertElementAt(j, index);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void removeDeadJob(Job j) {
    activeJobs.remove(j.jobName);
    sortedJobs.remove(j);
    //closeJobTrace(j);//csqs
  }
  
  private void closeJobTrace(Job j) {
      try {
          BufferedWriter output = new BufferedWriter(new FileWriter("result/trace/job-" + j.jobID + ".txt", true));
          String oneTrace = "JobID:" + j.jobID + "\nStartTime:" + j.actualStartTime + "\nSimulatedStartTime:" + j.simulatedStartTime + "\nSimulatedFinishTime:" + j.simulatedFinishTime + "\nMappersNum:" + j.numMappers + "\nReducers:" + j.numReducers + "\nTotalShuffleBytes:" + j.totalShuffleBytes + "\nMaxShuffleBytes:" + j.maxShuffleBytes + "\nJobDur:" + j.getSimulatedDuration() + "\n";
          output.write(oneTrace);
          output.close();
      } catch (Exception e) {
          System.out.println(e.toString());
      }
  }


  // proceed flow in transport layer
  
  private void proceedFlowsInAllRacks(long curTime, long quantaSize)
  {
    // defalut: quantaSize = 8ms, total_available_bandwidth = 1MB in 8ms
    // src/recv buffer size = 10MB
    // unit: MB -> packet, 1K packets (max) in one link, 10K packets (max) in one node
    // add -> flow, ecn_label; bps -> send_rate -> send_packet_ready
    // add -> node, loss, throughput  
    // 8ms -> order of leave the node, RR

    int[] src_buffer = new int[NUM_RACKS];
    int[] dest_buffer = new int[NUM_RACKS];

    //(Constants.RACK_BYTES_PER_SEC / 1024) / (Constants.SIMULATION_SECOND_MILLIS / quantaSize) = 1k
    int buffer_length = 1 * 1024;
    Arrays.fill(src_buffer, buffer_length);
    Arrays.fill(dest_buffer, buffer_length);

    int[] order_temp = new int[NUM_RACKS];
    for(int i = 0; i < NUM_RACKS; ++i)
      order_temp[i] = i;

    
    // here is still a gap; the leaving order should be packet-level, not node-level;
    // this random is for the sender buffer decreasing order
    // because this simulator is based on the receiver
    int[] leave_network_order = new int[NUM_RACKS];
    Random rand = new Random();
    for(int i = 0; i < NUM_RACKS; ++ i){
      int order_index = Math.abs(rand.nextInt() % (NUM_RACKS - i));
      leave_network_order[i] = order_temp[order_index];
      order_temp[order_index] = order_temp[NUM_RACKS - i - 1];
    }
    

    /*
    int[] leave_network_order = new int[NUM_RACKS];
    Random rand = new Random();
    for(int i = 0; i < NUM_RACKS; ++ i){
      leave_network_order[i] = order_temp[i];
    }
    */

    /*
    double[] src_loss_rate = new double[NUM_RACKS];
    double[] dest_loss_rate = new double[NUM_RACKS];
    double[] throughput = new double[NUM_RACKS];
    Arrays.fill(src_loss_rate, 0.0);
    Arrays.fill(dest_loss_rate, 0.0);
    Arrays.fill(throughput, 0.0);
    */
    long all_send_packets = 0;
    long all_throughput_packtes = 0;

    for(int i = 0; i < NUM_RACKS; ++i){
      int dest_index = leave_network_order[i];

      double totalBytesMoved = 0;
      Vector<Flow> flowsToRemove = new Vector<Flow>();

      // this random is for the receiver buffer
      // the order lets the CC work for different flows, which is close to the reality
      Vector<Flow> flows_order_temp = new Vector<Flow>();
      int dest_flows_number = 0;
      for (Flow f : flowsInRacks[dest_index]) {
        flows_order_temp.add(f);
        dest_flows_number += 1;
      }
      Vector<Flow> dest_flows_order = new Vector<Flow>();
      Random dest_rand = new Random();
      for(int j = 0; j < dest_flows_number; ++ j){
        // random state switch to 5
        if((curTime % (quantaSize * 2)) < Constants.ZERO){
          int order_index = Math.abs(dest_rand.nextInt() % (dest_flows_number - j));
          dest_flows_order.add(flows_order_temp.get(order_index));
          flows_order_temp.set(order_index, flows_order_temp.get(dest_flows_number - j - 1));
        } 
        else{
          dest_flows_order.add(flows_order_temp.get(j));
        }
      }
      

      //for (Flow f : flowsInRacks[dest_index]) {
      for (Flow f : dest_flows_order) {
        ReduceTask rt = f.reducer;

        //bandwidth per 1MB slice
        //double bytesPerTask = (f.currentBps / 8) * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS);
        double bytesPerTask = f.packetsNextToSend * 1024.0;
        bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);

        int packets_to_send = (int) (bytesPerTask / 1024.0);
        if(packets_to_send <= 1)
          packets_to_send = 1;

        int flow_src_id = f.mapper.taskID;

        /*
        dest_loss_rate[dest_index] += packets_to_send;
        src_loss_rate[flow_src_id] += packets_to_send;
        */
        all_send_packets += packets_to_send;

        int left_buffer_size = Math.min(src_buffer[flow_src_id], dest_buffer[dest_index]);

        
        // //TCP AIMD
        // if(packets_to_send > left_buffer_size){
        //   packets_to_send = left_buffer_size;
        //   f.packetsNextToSend = f.packetsNextToSend / 2;
        //   if(f.packetsNextToSend <= 1)
        //     f.packetsNextToSend = 1;
        // }
        // else{
        //   //here we set the max_win_size = 16
        //   if(f.packetsNextToSend < 16 && f.packetsNextToSend > 0){
            
        //     //MIMD
        //     if(f.packetsNextToSend * 2 < 16)
        //       f.packetsNextToSend *= 2;
        //     else
        //       f.packetsNextToSend += 1;
            
        //     //AIMD
        //     f.packetsNextToSend += 1;
        //   }
        //   else if(f.packetsNextToSend >= 16)
        //     f.packetsNextToSend = 16;
        //   else
        //     f.packetsNextToSend = 1;
        // }
        
        // //incast makes dctcp worse, may be better with background flows
        // //DCTCP
        // int ecn_thresh = (int) (buffer_length * 0.8);
        // if(buffer_length - left_buffer_size + packets_to_send <= ecn_thresh)
        //   f.ecn_frac = 0;
        // else{
        //   if(buffer_length - left_buffer_size >= ecn_thresh)
        //     f.ecn_frac = 0;
        //   else{
        //     int no_ecn_packets = ecn_thresh - (buffer_length - left_buffer_size);
        //     f.ecn_frac = 1 - (double) no_ecn_packets / (double) packets_to_send;
        //   }
        // }
        // f.dctcp_alpha = (1 - f.dctcp_g) * f.dctcp_alpha + f.dctcp_g * f.ecn_frac;

        // if(f.ecn_frac - 0 < Constants.ZERO){
        //   if(f.packetsNextToSend < 16 && f.packetsNextToSend > 0)
        //     f.packetsNextToSend += 1;
        //   else if(f.packetsNextToSend >= 16)
        //     f.packetsNextToSend = 16;
        //   else
        //     f.packetsNextToSend = 1;
        // }
        // else{
        //   f.packetsNextToSend = (int) (f.packetsNextToSend * (1 - f.dctcp_alpha / 2) + 0.5);
        // }

        // if(packets_to_send > left_buffer_size){
        //   packets_to_send = left_buffer_size;
        // }
        
        
        // //ICTCP
        // if(packets_to_send > left_buffer_size){
        //   packets_to_send = left_buffer_size;
        //   f.packetsNextToSend = packets_to_send;
        // }
        // else{
        //   //here we set the max_win_size = 16
        //   if(f.packetsNextToSend < 16 && f.packetsNextToSend > 0){
        //     //AIMD
        //     f.packetsNextToSend += 1;
        //   }
        //   else if(f.packetsNextToSend >= 16)
        //     f.packetsNextToSend = 16;
        //   else
        //     f.packetsNextToSend = 1;
        // }

        //BBR
        if(packets_to_send > left_buffer_size){
          packets_to_send = left_buffer_size;
          //f.packetsNextToSend = f.packetsNextToSend - packets_to_send;
        }
        else{
          int bbr_rtt_packets = (buffer_length - src_buffer[flow_src_id] + buffer_length - dest_buffer[dest_index]) / 2;
          //int bbr_rtt_packets = buffer_length - left_buffer_size;
          if(bbr_rtt_packets == 0)
            bbr_rtt_packets = 1;

          //bbr_rtt_packets = 1;
          int bbr_target_packets = buffer_length / bbr_rtt_packets;
          if(bbr_target_packets == 0)
            bbr_target_packets = 1;

          if(bbr_target_packets > packets_to_send){
            if(f.packetsNextToSend > 16)
              f.packetsNextToSend = (int)(f.packetsNextToSend * 1.25);
            else if(f.packetsNextToSend < 1)
              f.packetsNextToSend = 1;
            else
              f.packetsNextToSend = (int)(f.packetsNextToSend * 2.0);
          }
          else
            f.packetsNextToSend = bbr_target_packets;
        }


        // if(packets_to_send > left_buffer_size){
        //   packets_to_send = left_buffer_size;
        //   // f.packetsNextToSend = f.packetsNextToSend - packets_to_send; 
        // }
        // else{
        //   if(f.bbr_rtt_phrase == 8){
        //     f.packetsNextToSend = 1;
        //     packets_to_send = 1;
        //   }
        //   else{
        //     if(f.bbr_bandwidth_phrase == 2){
        //       if(f.packetsNextToSend > 16)
        //         f.packetsNextToSend = (int)(f.packetsNextToSend * 1.25);
        //       else if(f.packetsNextToSend < 1)
        //         f.packetsNextToSend = 1;
        //       else
        //         f.packetsNextToSend = (int)(f.packetsNextToSend * 2.0);
        //     }
        //     else{
        //       f.packetsNextToSend = f.packetsNextToSend;
        //     }
        //   }
        // }
        // f.bbr_rtt_phrase += 1;
        // f.bbr_bandwidth_phrase -= 1;
        // if(f.bbr_rtt_phrase == 9)
        //   f.bbr_rtt_phrase = 1;
        // if(f.bbr_bandwidth_phrase == 0)
        //   f.bbr_bandwidth_phrase = 2;

        /*
        throughput[flow_src_id] += packets_to_send * 1024.0;
        throughput[dest_index] += packets_to_send * 1024.0;
        */
        all_throughput_packtes += packets_to_send;

        
        src_buffer[flow_src_id] -= packets_to_send;
        dest_buffer[dest_index] -= packets_to_send;

        f.packetsRemaining -= packets_to_send;

        bytesPerTask = packets_to_send * 1024.0;
        f.bytesRemaining -= bytesPerTask;
        if (f.bytesRemaining <= Constants.ZERO) {
          rt.flows.remove(f);
          flowsToRemove.add(f);

          try {
            BufferedWriter output = new BufferedWriter(new FileWriter("result/flows_infor.txt", true));

            String oneTrace = rt.parentJob.jobID + "-" + f.totalBytes + "-" + (curTime - rt.parentJob.simulatedStartTime) + "\n";
            output.write(oneTrace);
            output.close();
          } catch (Exception e){
            System.out.println(e.toString());
          }

          // Give back to src and dst links
          //sendBpsFree[f.mapper.taskID] += f.currentBps;
          //recvBpsFree[f.reducer.taskID] += f.currentBps;
        }

        totalBytesMoved += bytesPerTask;
        rt.shuffleBytesLeft -= bytesPerTask;
        rt.parentJob.decreaseShuffleBytesPerRack(rt.taskID, bytesPerTask);
        rt.parentJob.shuffleBytesCompleted += bytesPerTask;

        // If no bytes remaining, mark end and mark for removal
        if ((rt.shuffleBytesLeft <= Constants.ZERO || rt.flows.size() == 0) && !rt.isCompleted()) {
          rt.cleanupTask(curTime + quantaSize);
          if (!rt.parentJob.jobActive) {
            removeDeadJob(rt.parentJob);
          }
          decNumActiveTasks();
        }
      }
      //}
      flowsInRacks[dest_index].removeAll(flowsToRemove);
    }

    /*
    if((curTime % (quantaSize * 10)) < Constants.ZERO){
      try {
          BufferedWriter output = new BufferedWriter(new FileWriter("result/network_board.txt", true));

          String oneTrace = "curtime: " + curTime + "\n";
          for(int i = 0; i < NUM_RACKS; ++ i){
            double loss_packets = src_loss_rate[i] - (double) buffer_length;
            if(loss_packets <= Constants.ZERO){
              src_loss_rate[i] = 0.0; 
            }
            else{
              src_loss_rate[i] = loss_packets / src_loss_rate[i];
            }

            loss_packets = dest_loss_rate[i] - (double) buffer_length;
            if(loss_packets <= Constants.ZERO){
              dest_loss_rate[i] = 0.0; 
            }
            else{
              dest_loss_rate[i] = loss_packets / src_loss_rate[i];
            }

            throughput[i] = throughput[i] / 2;

            oneTrace = oneTrace + i + "-" + src_loss_rate[i] + "-" + dest_loss_rate[i] + "-" + throughput[i] + "\n";
          }

          output.write(oneTrace);
          output.close();
        } catch (Exception e) {
          System.out.println(e.toString());
      }
    }
    */

     try {
          BufferedWriter output = new BufferedWriter(new FileWriter("result/network_board.txt", true));

          String oneTrace = all_send_packets + "-" + all_throughput_packtes + "\n";
          output.write(oneTrace);
          output.close();
        } catch (Exception e) {
          System.out.println(e.toString());
     }

  }  


  /*
  private void proceedFlowsInAllRacks(long curTime, long quantaSize) {
    for (int i = 0; i < NUM_RACKS; i++) {
      double totalBytesMoved = 0;
      Vector<Flow> flowsToRemove = new Vector<Flow>();
      for (Flow f : flowsInRacks[i]) {
        ReduceTask rt = f.reducer;

        if (totalBytesMoved >= Constants.RACK_BYTES_PER_SEC) {
          break;
        }

        //bandwidth per 1MB slice
        double bytesPerTask = (f.currentBps / 8)
            * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS);
        bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);

        f.bytesRemaining -= bytesPerTask;
        if (f.bytesRemaining <= Constants.ZERO) {
          rt.flows.remove(f);
          flowsToRemove.add(f);

          // Give back to src and dst links
          sendBpsFree[f.mapper.taskID] += f.currentBps;
          recvBpsFree[f.reducer.taskID] += f.currentBps;
        }

        totalBytesMoved += bytesPerTask;
        rt.shuffleBytesLeft -= bytesPerTask;
        rt.parentJob.decreaseShuffleBytesPerRack(rt.taskID, bytesPerTask);
        rt.parentJob.shuffleBytesCompleted += bytesPerTask;

        // If no bytes remaining, mark end and mark for removal
        if ((rt.shuffleBytesLeft <= Constants.ZERO || rt.flows.size() == 0) && !rt.isCompleted()) {
          rt.cleanupTask(curTime + quantaSize);
          if (!rt.parentJob.jobActive) {
            removeDeadJob(rt.parentJob);
          }
          decNumActiveTasks();
        }
      }
      flowsInRacks[i].removeAll(flowsToRemove);
    }
  }
  */

  protected void layoutFlowsInJobOrder() {
    for (int i = 0; i < NUM_RACKS; i++) {
      flowsInRacks[i].clear();
    }

    for (Job j : sortedJobs) {
      for (Task r : j.tasks) {
        if (r.taskType != TaskType.REDUCER) {
          continue;
        }

        ReduceTask rt = (ReduceTask) r;
        if (rt.isCompleted()) {
          continue;
        }

        flowsInRacks[r.taskID].addAll(rt.flows);
      }
    }
  }

  /**
   * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
   * by coflow length.
   */
  private static Comparator<Job> SCFComparator = new Comparator<Job>() {
    public int compare(Job o1, Job o2) {
      if (o1.maxShuffleBytes / o1.numMappers == o2.maxShuffleBytes / o2.numMappers) return 0;
      return o1.maxShuffleBytes / o1.numMappers < o2.maxShuffleBytes / o2.numMappers ? -1 : 1;
    }
  };

  /**
   * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
   * by coflow size.
   */
  private static Comparator<Job> LCFComparator = new Comparator<Job>() {
    public int compare(Job o1, Job o2) {
      double n1 = o1.calcShuffleBytesLeft();
      double n2 = o2.calcShuffleBytesLeft();
      if (n1 == n2) return 0;
      return n1 < n2 ? -1 : 1;
    }
  };

  /**
   * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
   * by the minimum number of endpoints of a coflow.
   */
  private static Comparator<Job> NCFComparator = new Comparator<Job>() {
    public int compare(Job o1, Job o2) {
      int n1 = (o1.numMappers < o1.numReducers) ? o1.numMappers : o1.numReducers;
      int n2 = (o2.numMappers < o2.numReducers) ? o2.numMappers : o2.numReducers;
      return n1 - n2;
    }
  };

  /**
   * Comparator used by {@link CoflowSimulator#addToSortedJobs(Job)} to add new job in list sorted
   * by static coflow skew.
   */
  private static Comparator<Job> SEBFComparator = new Comparator<Job>() {
    public int compare(Job o1, Job o2) {
      if (o1.alpha == o2.alpha) return 0;
      return o1.alpha < o2.alpha ? -1 : 1;
    }
  };
}
