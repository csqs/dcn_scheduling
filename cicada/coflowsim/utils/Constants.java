package coflowsim.utils;

import coflowsim.simulators.Simulator;

/**
 * Constants used throughout the simulator.
 */
public class Constants {

  /**
   * Scheduling/sharing algorithms supported by the {@link Simulator}
   */
  public enum SHARING_ALGO {
    /**
     * Flow-level fair sharing.
     */
    FAIR,
    /**
     * Per-flow SRTF priority and EDF for deadline-sensitive flows.
     */
    PFP,
    /**
     * First-In-First-Out at the coflow level.
     */
    FIFO,
    /**
     * Order coflows by length.
     */
    SCF,
    /**
     * Order coflows by width.
     */
    NCF,
    /**
     * Order coflows by total size.
     */
    LCF,
    /**
     * Order coflows by skew.
     */
    SEBF,
    /**
     * Use the non-clairvoyant scheduler.
     */
    DARK,
      /**
       * Use the non-clairvoyant scheduler with pattern.
       */
    SEBFC,
  }

  /**
   * For floating-point comparisons.
   */
  public static final double ZERO = 1e-3;

  /**
   * Constant for values we are not sure about.
   */
  public static final int VALUE_UNKNOWN = -1;

  /**
   * Constant for values we don't care about.
   */
  public static final int VALUE_IGNORED = -2;

  /**
   * Number of parallel flows initiated by each reducer.
   * Hadoop/Spark default is 5.
   */
  public static final int MAX_CONCURRENT_FLOWS = 5;

  /**
   * Capacity constraint of a rack in bps.
   */
  public static final double RACK_BITS_PER_SEC = 1.0 * 1024 * 1048576;

  /**
   * Capacity constraint of a rack in Bps.
   */
  public static final double RACK_BYTES_PER_SEC = RACK_BITS_PER_SEC / 8.0;

  /**
   * Number of milliseconds in a second of {@link Simulator}.
   * An epoch of {@link Simulator#simulate(int)}.
   */
  public static final int SIMULATION_SECOND_MILLIS = 1024;

  /**
   * Time step of {@link Simulator#simulate(int)}.
   */
  public static final int SIMULATION_QUANTA = SIMULATION_SECOND_MILLIS
      / (int) (RACK_BYTES_PER_SEC / 1048576);

  /**
   * {@link Simulator#simulate(int)} completes after this time.
   */
  public static final int SIMULATION_ENDTIME_MILLIS = 86400 * SIMULATION_SECOND_MILLIS;
  
  //csqs
  public static final int SIMULATION_SECOND_LPSTEP = 10;
  public static final int DEFAULT_FLOW_SIZE = 1048576;
  public static final double ALPHA_DIFF = 0.1;
  public static final double LOWBOUND_BPS = 1048576;
  public static final int SCHEDULE_COARSE = 10;
    
  public static final int FLOW_AVG = 1;
  public static final int FLOW_LINEAR = 2;
  public static final int FLOW_EXQ = 3;
    
  public static final int FLOW_DETECT = 1;
  public static final int FLOW_TRANSFER = 0;
    
  public static final int TOTAL_NUM_RACKS = 150;
    
}
