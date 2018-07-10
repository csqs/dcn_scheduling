package coflowsim.simulators;

import java.util.Arrays;
import java.util.Vector;

import coflowsim.datastructures.Flow;
import coflowsim.datastructures.Job;
import coflowsim.datastructures.ReduceTask;
import coflowsim.datastructures.Task;
import coflowsim.datastructures.Task.TaskType;
import coflowsim.traceproducers.TraceProducer;
import coflowsim.utils.Constants;
import coflowsim.utils.Constants.SHARING_ALGO;

//csqs
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Implements {@link coflowsim.simulators.Simulator} for flow-level scheduling policies (FAIR and
 * PFP).
 */
public class FlowSimulator extends Simulator {

  /**
   * {@inheritDoc}
   */
  public FlowSimulator(
      SHARING_ALGO sharingAlgo,
      TraceProducer traceProducer,
      boolean offline,
      boolean considerDeadline,
      double deadlineMultRandomFactor) {

    super(sharingAlgo, traceProducer, offline, considerDeadline, deadlineMultRandomFactor);
    assert (sharingAlgo == SHARING_ALGO.FAIR || sharingAlgo == SHARING_ALGO.PFP);
  }

  private void addAscending(Vector<Flow> coll, Vector<Flow> flows) {
    for (Flow f : flows) {
      addAscending(coll, f);
    }
  }

  private void addAscending(Vector<Flow> coll, Flow flow) {
    int index = 0;
    for (; index < coll.size(); index++) {
      if (coll.elementAt(index).bytesRemaining > flow.getFlowSize()) {
        break;
      }
    }
    flow.consideredAlready = true;
    coll.add(index, flow);
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
        }

        incNumActiveTasks();
      }
    }

    for (Task r : j.tasks) {
      if (r.taskType != TaskType.REDUCER) {
        continue;
      }
      ReduceTask rt = (ReduceTask) r;

      // Add at most Constants.MAX_CONCURRENT_FLOWS flows for FAIR sharing
      int numFlowsToAdd = rt.flows.size();
      if (sharingAlgo == SHARING_ALGO.FAIR) {
        numFlowsToAdd = Constants.MAX_CONCURRENT_FLOWS;
      }
      numFlowsToAdd = rt.flows.size();

      int added = 0;
      for (Flow f : rt.flows) {
        int toRack = rt.taskID;
        addAscending(flowsInRacks[toRack], f);

        added++;
        if (added >= numFlowsToAdd) {
          break;
        }
      }
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void onSchedule(long curTime) {
    //writeJobTrace(curTime);
    if (sharingAlgo == SHARING_ALGO.FAIR) {
      fairShare(curTime, Constants.SIMULATION_QUANTA);
    } else {
      proceedFlowsInAllRacksInSortedOrder(curTime, Constants.SIMULATION_QUANTA);
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

  /** {@inheritDoc} */
  @Override
  protected void removeDeadJob(Job j) {
    activeJobs.remove(j.jobName);
    closeJobTrace(j);//csqs
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

  /**
   * Flow-level fair sharing
   * 
   * @param curTime
   *          current time
   * @param quantaSize
   *          size of each simulation time step
   */
  private void fairShare(long curTime, long quantaSize) {
    // Calculate the number of outgoing flows
    int[] numMapSideFlows = new int[NUM_RACKS];
    Arrays.fill(numMapSideFlows, 0);
    for (int i = 0; i < NUM_RACKS; i++) {
      for (Flow f : flowsInRacks[i]) {
        numMapSideFlows[f.mapper.taskID]++;
      }
    }

    for (int i = 0; i < NUM_RACKS; i++) {
      Vector<Flow> flowsToRemove = new Vector<Flow>();
      Vector<Flow> flowsToAdd = new Vector<Flow>();
      for (Flow f : flowsInRacks[i]) {
        int numFlows = flowsInRacks[i].size();
        if (numFlows == 0) {
          continue;
        }

        ReduceTask rt = f.reducer;

        double bytesPerTask = Math.min(
            Constants.RACK_BYTES_PER_SEC * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS)
                / numFlows,
            Constants.RACK_BYTES_PER_SEC * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS)
                / numMapSideFlows[f.mapper.taskID]);

        bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);

        f.bytesRemaining -= bytesPerTask;
        if (f.bytesRemaining <= Constants.ZERO) {
          // Remove the one that has finished right now
          rt.flows.remove(f);
          flowsToRemove.add(f);

          // Remember flows to add, if available
          for (Flow ff : rt.flows) {
            if (!ff.consideredAlready) {
              flowsToAdd.add(ff);
              break;
            }
          }
        }

        rt.shuffleBytesLeft -= bytesPerTask;

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
      addAscending(flowsInRacks[i], flowsToAdd);
    }
  }

  /**
   * Proceed flows in each rack in the already-determined order; e.g., shortest-first of PFP or
   * earliest-deadline-first in the deadline-sensitive scenario.
   * 
   * @param curTime
   *          current time
   * @param quantaSize
   *          size of each simulation time step
   */
  private void proceedFlowsInAllRacksInSortedOrder(long curTime, long quantaSize) {
    boolean[] mapSideBusy = new boolean[NUM_RACKS];
    Arrays.fill(mapSideBusy, false);

    for (int i = 0; i < NUM_RACKS; i++) {
      Vector<Flow> flowsToRemove = new Vector<Flow>();
      for (Flow f : flowsInRacks[i]) {
        if (!mapSideBusy[f.mapper.taskID]) {
          mapSideBusy[f.mapper.taskID] = true;

          ReduceTask rt = f.reducer;

          double bytesPerTask = Constants.RACK_BYTES_PER_SEC
              * (1.0 * quantaSize / Constants.SIMULATION_SECOND_MILLIS);
          bytesPerTask = Math.min(bytesPerTask, f.bytesRemaining);

          f.bytesRemaining -= bytesPerTask;
          if (f.bytesRemaining <= Constants.ZERO) {
            rt.flows.remove(f);
            flowsToRemove.add(f);
          }

          rt.shuffleBytesLeft -= bytesPerTask;
          
          System.out.printf("--s.t.--Time:%l Job:%d ", curTime, rt.parentJob.jobID);//for schedule trace --- csqs
          rt.parentJob.decreaseShuffleBytesPerRack(rt.taskID, bytesPerTask);

          // If no bytes remaining, mark end and mark for removal
          if ((rt.shuffleBytesLeft <= Constants.ZERO || rt.flows.size() == 0)
              && !rt.isCompleted()) {

            rt.cleanupTask(curTime + quantaSize);
            if (!rt.parentJob.jobActive) {
              removeDeadJob(rt.parentJob);
            }
            decNumActiveTasks();
          }

          break;
        } else {
          System.out.print("");
        }
      }
      flowsInRacks[i].removeAll(flowsToRemove);
    }
  }
}
