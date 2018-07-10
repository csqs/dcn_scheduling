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
import coflowsim.datastructures.MapTask;
import coflowsim.datastructures.RackNode;
import java.util.*;
import java.util.Random;
//import java.lang.*;

/**
 * Implements {@link coflowsim.simulators.Simulator} for coflow-level scheduling policies (FIFO,
 * SCF, NCF, LCF, and SEBF).
 */
public class CoflowSimulatorCicada extends Simulator {

  Vector<Job> sortedJobs;
  double[] sendBpsFree;
  double[] recvBpsFree;
    
  //csqs
  Vector<Job> skippedJobs;
  int detectFlowNumInJobOne;
  int transferFlowNumInJobOne;
  int scheduleCoarse;
    
  Vector<Job> sortedKnownJobs;
  Vector<Job>[] sortedExqJobs;
  public static int NUM_JOB_QUEUES = 8;
  public static double INIT_QUEUE_LIMIT = 1048576.0 * 1;
  public static double JOB_SIZE_MULT = 10.0;


  /**
   * {@inheritDoc}
   */
  public CoflowSimulatorCicada(
      SHARING_ALGO sharingAlgo,
      TraceProducer traceProducer,
      boolean offline,
      boolean considerDeadline,
      double deadlineMultRandomFactor) {

    super(sharingAlgo, traceProducer, offline, considerDeadline, deadlineMultRandomFactor);
    assert (sharingAlgo == SHARING_ALGO.FIFO || sharingAlgo == SHARING_ALGO.SCF
        || sharingAlgo == SHARING_ALGO.NCF || sharingAlgo == SHARING_ALGO.LCF
        || sharingAlgo == SHARING_ALGO.SEBF|| sharingAlgo == SHARING_ALGO.SEBFC);
  }

  /** {@inheritDoc} */
  @Override
  protected void initialize(TraceProducer traceProducer) {
    super.initialize(traceProducer);

    this.sendBpsFree = new double[NUM_RACKS];
    this.recvBpsFree = new double[NUM_RACKS];
    resetSendRecvBpsFree();

    this.sortedJobs = new Vector<Job>();
      
    //csqs
    this.skippedJobs = new Vector<Job>();
    detectFlowNumInJobOne = 0;
    transferFlowNumInJobOne = 0;
    scheduleCoarse = Constants.SCHEDULE_COARSE;
    
    this.sortedExqJobs = (Vector<Job>[]) new Vector[NUM_JOB_QUEUES];
    for (int i = 0; i < NUM_JOB_QUEUES; i++) {
        sortedExqJobs[i] = new Vector<Job>();
    }
    
    this.sortedKnownJobs = new Vector<Job>();
  }

  protected void resetSendRecvBpsFree() {
    Arrays.fill(this.sendBpsFree, Constants.RACK_BITS_PER_SEC);
    Arrays.fill(this.recvBpsFree, Constants.RACK_BITS_PER_SEC);
  }

  //csqs
  /** {@inheritDoc} */
  @Override
  protected void uponJobAdmission(Job j) {
    //j.comPattern = getJobComPattern(j);
    double mtRatio = j.numMappers / j.numReducers;
    double mtRatioR = 1.0 / mtRatio;
    j.alpha = Math.max(mtRatio, 1.0 / mtRatio);
    
    if (!activeJobs.containsKey(j.jobName)){
          activeJobs.put(j.jobName, j);
    }
    for (Task t : j.tasks) {
        if (t.taskType == TaskType.REDUCER) {
            ReduceTask rt = (ReduceTask) t;
            rt.startTask(CURRENT_TIME);
            incNumActiveTasks();
        }
    }

    if(j.comPattern == 1){
        for (Task t : j.tasks) {
            if (t.taskType == Task.TaskType.MAPPER) {
                j.mapperTask.add(t);
            } else if (t.taskType == Task.TaskType.REDUCER) {
                j.reducerTask.add(t);
                ReduceTask rt = (ReduceTask) t;
                moveFlowsToTemp(rt);
            }
        }
        for (Task t : j.tasks) {
            if (t.taskType == Task.TaskType.REDUCER) {
                ReduceTask rt = (ReduceTask) t;
                for(Task mt : j.mapperTask){
                    rt.detectRemainMT.add(mt);
                }
            }
        }
        addToSortedJobs(j);
        detectBegin(j);
    }
    else{
        addToSortedExqJobs(j);
    }
    
  }
    
//  protected int getJobComPattern(Job j) {
//      if(j.jobID % 10 < 4){
//          return 1;
//      }
//      else{
//          return 0;
//      }
//  }
    
  protected void addToSortedJobs(Job j) {
    int index = 0;
    for (Job sj : sortedKnownJobs) {
      if (sharingAlgo == SHARING_ALGO.SEBFC && SEBFComparator.compare(j, sj) < 0) {
          break;
      }
      index++;
    }
    sortedKnownJobs.insertElementAt(j, index);
  }

    
  protected void addToSortedExqJobs(Job j) {
      double maxBytes = j.alpha * Constants.DEFAULT_FLOW_SIZE;
      int curQ = 0;
      for (double k = INIT_QUEUE_LIMIT; k < maxBytes && (curQ + 1) < NUM_JOB_QUEUES; k *= JOB_SIZE_MULT) {
          curQ += 1;
      }
      sortedExqJobs[curQ].add(j);
  }
    
    /** {@inheritDoc} */
  @Override
  protected void removeDeadJob(Job j) {
      activeJobs.remove(j.jobName);
      if(j.comPattern == 1){
          sortedKnownJobs.remove(j);
      }
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
          if (sortedExqJobs[i].remove(j)) {
              break;
          }
      }
      finishedJob += 1;//csqs
      //closeJobTrace(j);//csqs
  }
    
  //csqs
  protected void moveFlowsToTemp(ReduceTask rt){
     for(Flow f : rt.flows){
        rt.tempFlows.add(f);
     }
     rt.flows.clear();
  }
    
  //csqs
  protected void detectBegin(Job j){
    //System.err.printf("function in detectBegin\n");
    Vector<Task> detectRemainTask = new Vector<Task>();
    for(Task t : j.reducerTask){
      if(!j.detectTouchedRT.contains(t))
          detectRemainTask.add(t);
    }
    
    if(detectRemainTask.size() > 0){
        for (Task t : detectRemainTask) {
            ReduceTask rt = (ReduceTask) t;
            Vector<Task> detectRMTask = new Vector<Task>();
            for(Task mt : rt.detectRemainMT){
                detectRMTask.add(mt);
            }
            
            for(Task mt : j.mapperTask){
                if(j.detectActiveMT.contains(mt))
                    detectRMTask.remove(mt);
            }
            
            if(detectRMTask.size() == 0){
                break;
            }
            
            
            int rackID = t.taskID;
            int mapID = -1;
            RackNode rackOne = netRacks.get(rackID);
            
            if(rackOne.detectFlows.size() > 0){
                double jobAlpha = -1;
                Vector<Integer> mapTID = new Vector<Integer>();
                for (Task mtt : detectRMTask)
                    mapTID.add(mtt.taskID);
                for(Flow f : rackOne.detectFlows){
                    int detectFlowMapID = f.mapper.taskID;
                    if(mapTID.contains(detectFlowMapID) && j.alpha > jobAlpha){
                        jobAlpha = j.alpha;
                        mapID = detectFlowMapID;
                    }
                }
            }
            
            if(mapID == -1){
                double bandwidthRemain = -1;
                for(Task mtt : detectRMTask){
                    int mID = mtt.taskID;
                    if(sendBpsFree[mID] > bandwidthRemain){
                        mapID = mID;
                        bandwidthRemain = sendBpsFree[mID];
                    }
                }

//                Random rand = new Random( );
//                int randomTask = rand.nextInt(detectRMTask.size());
//                for (Task mtt : detectRMTask){
//                    randomTask -= 1;
//                    if(randomTask < 0){
//                        mapID = mtt.taskID;
//                    }
//                }
            }
            
            for(Task mtt : detectRMTask){
                if(mtt.taskID == mapID){
                    MapTask mt = (MapTask) mtt;
                    
                    j.detectActiveMT.add(mt);
                    j.detectTouchedRT.add(t);
                    rt.detectRemainMT.remove(mt);
                    
                    Flow oneDetectFlow = getFlowInTemp(mt, rt);
                    rackOne.detectFlows.add(oneDetectFlow);
                    j.detectFlows.add(oneDetectFlow);
                    //detectFlowNumInJobOne += 1;
                    break;
                }
            }
        }
    }

  }
  //csqs
  protected Flow getFlowInTemp(MapTask mt, ReduceTask rt){
      Flow oneDetectFlow = new Flow(mt, rt, Constants.DEFAULT_FLOW_SIZE);
      if(!rt.sizeIsKnown){
          for(Flow f : rt.tempFlows){
              if(f.mapper.taskID == mt.taskID){
                  rt.flows.add(f);
                  oneDetectFlow = f;
                  oneDetectFlow.flowState = 1;
                  break;
              }
          }
          rt.tempFlows.remove(oneDetectFlow);
      }
      return oneDetectFlow;
  }

  /** {@inheritDoc} */
  @Override
  protected void afterJobAdmission(long curTime) {
      updateOrderAndRate(curTime);
  }

  /** {@inheritDoc} */
  @Override
  protected void onSchedule(long curTime) {
      onDetailedSchedule(curTime, false, false);
  }
    
  protected void updateOrderAndRate(long curTime) {
      resetSendRecvBpsFree();
      //updateRatesAlpha(curTime);
      
      updateSortedUnKnownJobs();
      updateDynamicAlpha(curTime);
      updateSortedKnownJobs();
      
      updateRateUnKnownJobs();
      
      mergeToSortedJobs();
      updateRatesWorkConservation(curTime);
  }
    
  protected void onDetailedSchedule(long curTime, boolean writeFile, boolean coarse){
      if(writeFile){
          writeJobTrace(curTime);
      }
      if(coarse){
          if(scheduleCoarse > 0){
            scheduleCoarse -= 1;
          }
          else{
            scheduleCoarse = Constants.SCHEDULE_COARSE;
            updateOrderAndRate(curTime);
          }
          
      }
      else{
          updateOrderAndRate(curTime);
      }
      
      layoutFlowsInJobOrder();
      proceedFlowsInAllRacks(curTime, Constants.SIMULATION_QUANTA);
      
  }

  /** {@inheritDoc} */
  @Override
  protected void afterJobDeparture(long curTime) {
      updateOrderAndRate(curTime);
  }

  public void mergeToSortedJobs(){
    for (int i = 0; i < NUM_JOB_QUEUES; i++) {
        for (Job j : sortedExqJobs[i]) {
            sortedJobs.add(j);
        }
    }
  }
    
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
    sortedJobs.clear();
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
        if(rt.sizeIsKnown){
            recvBytes[rt.taskID] = rt.shuffleBytesLeft;
            for (Flow f : rt.flows) {
              sendBytes[f.mapper.taskID] += f.bytesRemaining;
            }
        }
        else{
            recvBytes[rt.taskID] = rt.sentBytes * (rt.parentJob.numMappers - 1);
            for (Task mt : rt.parentJob.mapperTask) {
              sendBytes[mt.taskID] += rt.sentBytes;
            }
        }
      }
    }

    // Scale by available capacity
    for (int i = 0; i < NUM_RACKS; i++) {
//      sendBytes[i] = sendBytes[i] * 8 / sFree[i];
//      recvBytes[i] = recvBytes[i] * 8 / rFree[i];
      //csqs
      sendBytes[i] = sendBytes[i] * 8 / Constants.RACK_BITS_PER_SEC;
      recvBytes[i] = recvBytes[i] * 8 / Constants.RACK_BITS_PER_SEC;
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
  private void updateDynamicAlpha(final long curTime) {
    for (Job sj : sortedKnownJobs) {
      sj.alpha = calcAlphaOnline(sj, sendBpsFree, recvBpsFree);
    }
  }
    
  private void updateRatesAlpha(final long curTime) {
    for (Job sj : sortedKnownJobs) {
        double[] sendUsed = new double[NUM_RACKS];
        double[] recvUsed = new double[NUM_RACKS];
        Arrays.fill(sendUsed, 0.0);
        Arrays.fill(recvUsed, 0.0);
        
        //updateRatesDynamicAlphaOneJob(sj, sj.alpha, sendUsed, recvUsed);
        
        for (int i = 0; i < NUM_RACKS; i++) {
            sendBpsFree[i] -= sendUsed[i];
            if(sendBpsFree[i] < Constants.ZERO)
                sendBpsFree[i] = Constants.ZERO;
            recvBpsFree[i] -= recvUsed[i];
            if(recvBpsFree[i] < Constants.ZERO)
                recvBpsFree[i] = Constants.ZERO;
        }
    }
  }
    
  private void updateRatesWorkConservation(final long curTime) {
      Vector<Job> sortedByEDF = new Vector<Job>(sortedJobs);
      Collections.sort(sortedByEDF, new Comparator<Job>() {
          public int compare(Job o1, Job o2) {
              int timeLeft1 = (int) (o1.simulatedStartTime + o1.deadlineDuration - curTime);
              int timeLeft2 = (int) (o2.simulatedStartTime + o2.deadlineDuration - curTime);
              return timeLeft1 - timeLeft2;
          }
      });
      
      for (Job sj : sortedByEDF) {
//          if((curTime / Constants.SIMULATION_SECOND_MILLIS) > 3600){
//              System.err.printf("function in updateRatesWorkConservation:%d\n", sj.jobID);
//          }
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
                  //System.err.printf("f.currentBps: %5f function in Work conservationb\n", f.currentBps);
              }
          }
      }
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
   * @return
   */
  private void updateRatesDynamicAlphaOneJob(Job sj) {
      int detectFlowNumINBps = 0;
      double[] sendUsed = new double[NUM_RACKS];
      double[] recvUsed = new double[NUM_RACKS];
      Arrays.fill(sendUsed, 0.0);
      Arrays.fill(recvUsed, 0.0);
      //csqs,detect
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
              double curBps = 0.0;
          
              if (Math.min(sendBpsFree[src], recvBpsFree[dst]) <= Constants.ZERO) {
                  //curBps = zeroBpsAvoid(Flow f);
                  //curBps = Constants.LOWBOUND_BPS;
                  //skippedJobs.add(sj);
                  curBps = 0.0;
                  f.currentBps = curBps;
                  continue;
              }
          
              if(f.flowState > 0){
                  curBps = Math.min(sendBpsFree[src], recvBpsFree[dst]);
                  sendUsed[src] += curBps;
                  recvUsed[dst] += curBps;
                  detectFlowNumINBps += 1;
                  f.currentBps = curBps;
              }
          }
      }
   //csqs, transfer
      for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) {
            continue;
          }
          
          ReduceTask rt = (ReduceTask) t;
          int dst = rt.taskID;
          if (recvUsed[dst] > recvBpsFree[dst]) {
            continue;
          }
          
         for (Flow f : rt.flows) {
           int src = f.mapper.taskID;
           double curBps = 0.0;

           if(f.flowState == 0){
             if (sendUsed[src] > sendBpsFree[src]) {
                  //System.err.printf("src: %5d function in updateRatesDynamicAlphaOneJob\n", src);
                  //curBps = zeroBpsAvoid(Flow f);
                  //curBps = Constants.LOWBOUND_BPS;
                  //skippedJobs.add(sj);
                 curBps = 0.0;
                 f.currentBps = curBps;
                 continue;
             }
             else{
                 curBps = f.bytesRemaining * 8 / sj.alpha;
                 //f.alphaBps = curBps;
                 if(((sendUsed[src] + curBps) > sendBpsFree[src]) || ((recvUsed[dst] + curBps) > recvBpsFree[dst])){
                     double curBpsSend = sendBpsFree[src] - sendUsed[src];
                     double curBpsRecv = recvBpsFree[dst] - recvUsed[dst];
                     curBps = Math.min(curBpsSend, curBpsRecv);
                     f.currentBps = curBps;
                 }
                 else{
                     f.currentBps = curBps;
                 }
                 sendUsed[src] += curBps;
                 recvUsed[dst] += curBps;
             }
           }
         }
      }
      for (int i = 0; i < NUM_RACKS; i++){
          sendBpsFree[i] -= sendUsed[i];
          if(sendBpsFree[i] < Constants.ZERO)
              sendBpsFree[i] = Constants.ZERO;
          recvBpsFree[i] -= recvUsed[i];
          if(recvBpsFree[i] < Constants.ZERO)
              recvBpsFree[i] = Constants.ZERO;
      }
  }

  //csqs
  protected void updateSortedUnKnownJobs() {
      Vector<Job> jobInQueue = new Vector<Job>();
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
          for (Job j : sortedExqJobs[i]) {
             jobInQueue.add(j);
          }
      }
      for(Job j : jobInQueue){
          updateOrderOneJob(j);
      }
      
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
          Collections.sort(sortedExqJobs[i], new Comparator<Job>() {
              public int compare(Job o1, Job o2) {
                  int timeLeft1 = (int) (o1.simulatedStartTime);
                  int timeLeft2 = (int) (o2.simulatedStartTime);
                  return timeLeft1 - timeLeft2;
              }
          });
      }
  }
    
  private void updateOrderOneJob(Job sj){
      double[] sendBytes = new double[NUM_RACKS];
      Arrays.fill(sendBytes, 0.0);
      double[] recvBytes = new double[NUM_RACKS];
      Arrays.fill(recvBytes, 0.0);
      for (Task t : sj.tasks) {
          if (t.taskType == TaskType.REDUCER) {
              ReduceTask rt = (ReduceTask) t;
              for (int index = 0; index < NUM_RACKS; index++) {
                  double flowSendBytes = rt.mapperSentBytes[index];
                  if(flowSendBytes != 0.0){
                      recvBytes[rt.taskID] += flowSendBytes;
                      sendBytes[index] += flowSendBytes;
                  }
              }
          }
      }
      double maxBytes = Math.max(Utils.max(sendBytes), Utils.max(recvBytes));
      int lastQ = -1, curQ = 0;
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
          for (Job j : sortedExqJobs[i]) {
              if(j.jobID == sj.jobID){
                  lastQ = i;
              }
          }
      }
      for (double k = INIT_QUEUE_LIMIT; k < maxBytes && (curQ + 1) < NUM_JOB_QUEUES; k *= JOB_SIZE_MULT) {
          curQ += 1;
      }
      if(lastQ == -1){
          sortedExqJobs[curQ].add(sj);
      }
      else if(lastQ != curQ){
          sortedExqJobs[lastQ].remove(sj);
          sortedExqJobs[curQ].add(sj);
      }
  }
    
  protected void updateRateUnKnownJobs() {
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
         for (Job j : sortedExqJobs[i]) {
             if(j.comPattern == 1){
                 //updateRateOneSmallJob(j);
                 updateRatesDynamicAlphaOneJob(j);
             }
             else{
                 //updateRateOneSmallJob(j);
                 if(i < 2){
                     updateRateOneSmallJob(j);
                 }
                 else if( i <5){
                     updateRateOneMedianJob(j);
                 }
                 else{
                     //updateRateOneMedianJob(j);
                     updateRateOneLargeJob(j);
                 }
             }
         }
      }
  }

  private void updateRateOneSmallJob(Job sj){
      // Calculate the number of mappers and reducers in each port
      int[] numMapSideFlows = new int[NUM_RACKS];
      Arrays.fill(numMapSideFlows, 0);
      int[] numReduceSideFlows = new int[NUM_RACKS];
      Arrays.fill(numReduceSideFlows, 0);
      for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) continue;
          
          ReduceTask rt = (ReduceTask) t;
          int dst = rt.taskID;
          if (recvBpsFree[dst] <= Constants.ZERO) {
              // Set rates to 0 explicitly else it may send at the rate it was assigned last
              for (Flow f : rt.flows) {
                  f.currentBps = 0;
              }
              continue;
          }
          
          for (Flow f : rt.flows) {
              int src = f.mapper.taskID;
              if (sendBpsFree[src] <= Constants.ZERO) {
                  // Set rates to 0 explicitly else it may send at the rate it was assigned last
                  f.currentBps = 0;
                  continue;
              }
              
              numMapSideFlows[src]++;
              numReduceSideFlows[dst]++;
          }
      }
      
      for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) continue;
          
          ReduceTask rt = (ReduceTask) t;
          int dst = rt.taskID;
          if (recvBpsFree[dst] <= Constants.ZERO || numReduceSideFlows[dst] == 0) continue;
          
          for (Flow f : rt.flows) {
              int src = f.mapper.taskID;
              if (sendBpsFree[src] <= Constants.ZERO || numMapSideFlows[src] == 0) continue;
              
              // Determine rate based only on this job and available bandwidth
              double minFree = Math.min(sendBpsFree[src] / numMapSideFlows[src],
                                        recvBpsFree[dst] / numReduceSideFlows[dst]);
              if (minFree <= Constants.ZERO) {
                  minFree = 0.0;
              }
              
              f.currentBps = minFree;
              
              // Remember how much capacity was allocated
              sendBpsFree[src] -= f.currentBps;
              recvBpsFree[dst] -= f.currentBps;
          }
      }
  }
    
  private void updateRateOneMedianJob(Job sj){
      Vector<ReduceTask> remainTask = new Vector<ReduceTask>();
      for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) continue;
          ReduceTask rt = (ReduceTask) t;
          if(!rt.isCompleted()){
              remainTask.add(rt);
          }
      }
      
      Collections.sort(remainTask, new Comparator<ReduceTask>() {
          public int compare(ReduceTask o1, ReduceTask o2) {
              if(o1.sentBytes == o2.sentBytes){
                  return 0;
              }
              return o1.sentBytes > o2.sentBytes ? -1 : 1;
          }
      });
      
      for (ReduceTask rt : remainTask) {
          Vector<Flow> sortedFlows = new Vector<Flow>(rt.flows);
          Collections.sort(sortedFlows, new Comparator<Flow>() {
              public int compare(Flow o1, Flow o2) {
                  if(o1.sentBytes == o2.sentBytes){
                      return 0;
                  }
                  return o1.sentBytes > o2.sentBytes ? -1 : 1;
              }
          });
          
          double remainSendBytes = Constants.ZERO;
          for(Flow f : sortedFlows){
              remainSendBytes += f.sentBytes;
          }
          for(Flow f : sortedFlows){
              int dst = rt.taskID;
              int src = f.mapper.taskID;
              
              if (recvBpsFree[dst] <= Constants.ZERO){
                  f.currentBps = 0;
                  break;
              }
              if (sendBpsFree[src] <= Constants.ZERO){
                  f.currentBps = 0;
                  continue;
              }

              double ratioBps = recvBpsFree[dst] * (f.sentBytes / remainSendBytes);
              f.currentBps = Math.min(ratioBps, sendBpsFree[src]);
              
              sendBpsFree[src] -= f.currentBps;
              recvBpsFree[dst] -= f.currentBps;
          }
      }
  }
    
  private void updateRateOneLargeJob(Job sj){
      Vector<ReduceTask> remainTask = new Vector<ReduceTask>();
      for (Task t : sj.tasks) {
          if (t.taskType != TaskType.REDUCER) continue;
          ReduceTask rt = (ReduceTask) t;
          if(!rt.isCompleted()){
              remainTask.add(rt);
          }
      }
        
      Collections.sort(remainTask, new Comparator<ReduceTask>() {
          public int compare(ReduceTask o1, ReduceTask o2) {
              if(o1.sentBytes == o2.sentBytes){
                  return 0;
              }
              return o1.sentBytes > o2.sentBytes ? -1 : 1;
          }
      });
        
      for (ReduceTask rt : remainTask) {
          Vector<Flow> sortedFlows = new Vector<Flow>(rt.flows);
          Collections.sort(sortedFlows, new Comparator<Flow>() {
              public int compare(Flow o1, Flow o2) {
                  if(o1.sentBytes == o2.sentBytes){
                      return 0;
                  }
                  return o1.sentBytes > o2.sentBytes ? -1 : 1;
              }
          });
            
          for(Flow f : sortedFlows){
              int dst = rt.taskID;
              int src = f.mapper.taskID;
                
              if (recvBpsFree[dst] <= Constants.ZERO){
                  f.currentBps = 0;
                  break;
              }
              if (sendBpsFree[src] <= Constants.ZERO){
                  f.currentBps = 0;
                  continue;
              }
              
              f.currentBps = Math.min(recvBpsFree[dst], sendBpsFree[src]);

              sendBpsFree[src] -= f.currentBps;
              recvBpsFree[dst] -= f.currentBps;
          }
      }
  }
    
  protected void updateSortedKnownJobs() {
      Vector<Job> sortedJobsTemp = new Vector<Job>();
      for (Job j : sortedKnownJobs) {
          if(sortedJobsTemp.size() == 0){
             sortedJobsTemp.add(j);
          } else{
            int index = 0;
            for (Job sj : sortedJobsTemp){
                if (sharingAlgo == SHARING_ALGO.SEBF && SEBFComparator.compare(j, sj) < 0) {
                    break;
                }else if (sharingAlgo == SHARING_ALGO.SEBFC && SEBFComparator.compare(j, sj) < 0) {
                    break;
                }
                index++;
            }
            sortedJobsTemp.insertElementAt(j, index);
        }
      }
      sortedKnownJobs = new Vector<Job>();
      for (Job j : sortedJobsTemp) {
          sortedKnownJobs.add(j);
      }
      
      Vector<Job>[] sortedExqKnownJobs = (Vector<Job>[]) new Vector[NUM_JOB_QUEUES];
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
          sortedExqKnownJobs[i] = new Vector<Job>();
      }
      for (Job sj : sortedKnownJobs) {
          double maxBytes = sj.alpha * Constants.RACK_BITS_PER_SEC / 8;
          int lastQ = -1, curQ = 0;
          for (int i = 0; i < NUM_JOB_QUEUES; i++) {
              for (Job j : sortedExqJobs[i]) {
                  if(j.jobID == sj.jobID){
                      lastQ = i;
                  }
              }
          }
          for (double k = INIT_QUEUE_LIMIT; k < maxBytes && (curQ + 1) < NUM_JOB_QUEUES; k *= JOB_SIZE_MULT) {
              curQ += 1;
          }
          if(lastQ == -1){
              sortedExqKnownJobs[curQ].add(sj);
          }
          else if(lastQ != curQ){
              sortedExqJobs[lastQ].remove(sj);
              sortedExqKnownJobs[curQ].add(sj);
          }
      }
      
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
          for (Job j : sortedExqJobs[i]){
              sortedExqKnownJobs[i].add(j);
          }
      }
      
      for (int i = 0; i < NUM_JOB_QUEUES; i++) {
          sortedExqJobs[i].clear();
          for (Job j : sortedExqKnownJobs[i]){
              sortedExqJobs[i].add(j);
          }
      }

  }
    
  //csqs
  private void writeJobTrace(long curTime) {
      for (Entry<String, Job> entry : activeJobs.entrySet()) {
          Job oneJob = entry.getValue();
          try {
              BufferedWriter output = new BufferedWriter(new FileWriter("result/trace/job-" + oneJob.jobID + ".txt", true));
              for (int i = 0; i < NUM_RACKS; i++) {
                  if(oneJob.shuffleBytesPerRack[i] > 0.0){
                      String oneTrace = "Time:" + curTime + " RackID:" + i + " RackLeft:" + oneJob.shuffleBytesPerRack[i] + "\n";
                      output.write(oneTrace);
                  }
              }
              output.close();
          } catch (Exception e) {
              System.out.println(e.toString());
          }
      }
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

        // if((curTime % (quantaSize * 2)) < Constants.ZERO){
        //   int order_index = Math.abs(dest_rand.nextInt() % (dest_flows_number - j));
        //   dest_flows_order.add(flows_order_temp.get(order_index));
        //   flows_order_temp.set(order_index, flows_order_temp.get(dest_flows_number - j - 1));
        // } 
        // else{
        //   dest_flows_order.add(flows_order_temp.get(j));
        // }
        // dest_flows_order.add(flows_order_temp.get(j));

        int order_index = Math.abs(dest_rand.nextInt() % (dest_flows_number - j));
        dest_flows_order.add(flows_order_temp.get(order_index));
        flows_order_temp.set(order_index, flows_order_temp.get(dest_flows_number - j - 1));
      }
      

      //for (Flow f : flowsInRacks[dest_index]) {
      for (Flow f : dest_flows_order) {
        ReduceTask rt = f.reducer;

        //bandwidth per 1MB slice
        //double bytesPerTask = (f.currentBps / 8) * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS);
        double bytesPerTask = f.packetsNextToSend * 1024.0;
        //bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);

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
        //     //f.packetsNextToSend += 1;
        //   }
        //   else if(f.packetsNextToSend >= 16)
        //     f.packetsNextToSend = 16;
        //   else
        //     f.packetsNextToSend = 1;
        // }

        // // incast makes dctcp worse, may be better with background flows
        // // DCTCP
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
        //   // f.packetsNextToSend *= 2;
        //   // ecn_thresh = (int) (buffer_length * 0.4);
        //   // if(f.packetsNextToSend > ecn_thresh){
        //   //   f.packetsNextToSend = ecn_thresh;
        //   // }
        //   if(f.packetsNextToSend < 16 && f.packetsNextToSend > 0)
        //     f.packetsNextToSend *= 2;
        //   else if(f.packetsNextToSend >= 16)
        //     f.packetsNextToSend = 16;
        //   else
        //     f.packetsNextToSend = 1;
        // }
        // else{
        //   if(packets_to_send > left_buffer_size){
        //     packets_to_send = left_buffer_size;
        //     f.packetsNextToSend = f.packetsNextToSend / 2;
        //     if(f.packetsNextToSend <= 1)
        //       f.packetsNextToSend = 1;
        //   }
        //   else{
        //     f.packetsNextToSend = (int) (f.packetsNextToSend * (1 - f.dctcp_alpha / 2) + 0.5);
        //   }
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
        int bbr_rtt_packets = (buffer_length - src_buffer[flow_src_id] + buffer_length - dest_buffer[dest_index]) / 2;
          //int bbr_rtt_packets = buffer_length - left_buffer_size;
        if(bbr_rtt_packets == 0)
          bbr_rtt_packets = 1;

        //bbr_rtt_packets = 1;
        double bbr_target_packets_double = 0.8 * (double)buffer_length / (double)bbr_rtt_packets;
        //int bbr_target_packets =  0.8 * buffer_length / bbr_rtt_packets;
        int bbr_target_packets = (int)bbr_target_packets_double;
        if(bbr_target_packets == 0)
          bbr_target_packets = 1;

        if(packets_to_send > left_buffer_size){
          packets_to_send = left_buffer_size;
          if(bbr_target_packets > packets_to_send)
            f.packetsNextToSend = packets_to_send;
          else
            f.packetsNextToSend = bbr_target_packets;
        }
        else{
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
        
        
        // //ICTCP
        // if(f.flowState == 0){
        //   if(packets_to_send > left_buffer_size){
        //     packets_to_send = left_buffer_size;
        //     f.packetsNextToSend = packets_to_send;
        //   }
        //   else{
        //     int bbr_rtt_packets = (buffer_length - src_buffer[flow_src_id] + buffer_length - dest_buffer[dest_index]) / 2;
        //     //int bbr_rtt_packets = buffer_length - left_buffer_size;
        //     if(bbr_rtt_packets == 0)
        //       bbr_rtt_packets = 1;

        //     //bbr_rtt_packets = 1;
        //     int bbr_target_packets = 32 * buffer_length / bbr_rtt_packets;
        //     if(bbr_target_packets == 0)
        //       bbr_target_packets = 1;

        //     if(bbr_target_packets > packets_to_send){
        //       if(f.packetsNextToSend > 16)
        //         f.packetsNextToSend = (int)(f.packetsNextToSend * 1.25);
        //       else if(f.packetsNextToSend < 1)
        //         f.packetsNextToSend = 1;
        //       else
        //         f.packetsNextToSend = (int)(f.packetsNextToSend * 2.0);
        //     }
        //     else
        //       f.packetsNextToSend = bbr_target_packets;
        //   }
        // }
        // else{
        //   //BBR
        //   if(packets_to_send > left_buffer_size){
        //     packets_to_send = left_buffer_size;
        //     //f.packetsNextToSend = f.packetsNextToSend - packets_to_send;
        //   }
        //   else{
        //     int bbr_rtt_packets = (buffer_length - src_buffer[flow_src_id] + buffer_length - dest_buffer[dest_index]) / 2;
        //     //int bbr_rtt_packets = buffer_length - left_buffer_size;
        //     if(bbr_rtt_packets == 0)
        //       bbr_rtt_packets = 1;

        //     bbr_rtt_packets = 1;
        //     int bbr_target_packets = buffer_length / bbr_rtt_packets;
        //     if(bbr_target_packets == 0)
        //       bbr_target_packets = 1;

        //     if(bbr_target_packets > packets_to_send){
        //       if(f.packetsNextToSend > 16)
        //         f.packetsNextToSend = (int)(f.packetsNextToSend * 1.25);
        //       else if(f.packetsNextToSend < 1)
        //         f.packetsNextToSend = 1;
        //       else
        //         f.packetsNextToSend = (int)(f.packetsNextToSend * 2.0);
        //     }
        //     else
        //       f.packetsNextToSend = bbr_target_packets;
        //   }
        // }

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

        f.sentBytes += bytesPerTask; //csqs
        rt.sentBytes += bytesPerTask;

        if (f.bytesRemaining <= Constants.ZERO) {
          rt.flows.remove(f);
          flowsToRemove.add(f);

          rt.parentJob.flowFinishedBytes += f.totalBytes;//csqs
          rt.parentJob.flowFinished += 1;//csqs
          rt.sizeKnown.add(f.sentBytes);
          rt.detectRemainMT.remove(f.mapper);

          //csqs
          if(!rt.sizeIsKnown && rt.parentJob.comPattern == 1){
            finishedFlowStateTransfer(f);
          }

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
        //csqs
        if(!rt.sizeIsKnown){
            //rt.sentBytes += bytesPerTask;
            rt.mapperSentBytes[f.mapper.taskID] += bytesPerTask;
        }
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

        double bytesPerTask = (f.currentBps / 8)
            * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS);
        bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining); //csqs
        
        f.bytesRemaining -= bytesPerTask;
        f.sentBytes += bytesPerTask; //csqs
        rt.sentBytes += bytesPerTask;
          
        if (f.bytesRemaining <= Constants.ZERO) {
          rt.flows.remove(f);
          flowsToRemove.add(f);
          rt.parentJob.flowFinishedBytes += f.totalBytes;//csqs
          rt.parentJob.flowFinished += 1;//csqs
          rt.sizeKnown.add(f.sentBytes);
          rt.detectRemainMT.remove(f.mapper);

          // Give back to src and dst links
          sendBpsFree[f.mapper.taskID] += f.currentBps;
          recvBpsFree[f.reducer.taskID] += f.currentBps;
          
          //csqs
          if(!rt.sizeIsKnown && rt.parentJob.comPattern == 1){
            finishedFlowStateTransfer(f);
          }
        }

        totalBytesMoved += bytesPerTask;
        rt.shuffleBytesLeft -= bytesPerTask;
        //csqs
        if(!rt.sizeIsKnown){
            //rt.sentBytes += bytesPerTask;
            rt.mapperSentBytes[f.mapper.taskID] += bytesPerTask;
        }
          
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
  
  //csqs
  private void finishedFlowStateTransfer(Flow f) {
      //System.err.printf("function in finishedFlowStateTransfer\n");
      ReduceTask rt = f.reducer;
      RackNode rackOne = netRacks.get(rt.taskID);
      if(rt.parentJob.detectFlows.contains(f)){
          rt.parentJob.detectActiveMT.remove(f.mapper);
          rackOne.detectFlows.remove(f);
          rt.parentJob.detectFlows.remove(f);
          
          //now the size of reducerTask is known
          rt.sizeIsKnown = true;
          //rt.sentBytes = rt.shuffleBytes;
          
          if(rt.parentJob.detectTouchedRT.size() < rt.parentJob.reducerTask.size()){
              detectBegin(rt.parentJob);
          }
          
          //double flowSize = Math.max(rt.shuffleBytes / rt.parentJob.numMappers, Constants.DEFAULT_FLOW_SIZE);
          moveTempToFlows(rt);
      }
  }
    
  //csqs
  protected void moveTempToFlows(ReduceTask rt){
    for(Flow f : rt.tempFlows){
        //System.err.printf("function in moveTempToFlows\n");
        f.flowState = 0;
        rt.flows.add(f);
    }
    rt.tempFlows.clear();
  }


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
