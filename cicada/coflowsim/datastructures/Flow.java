package coflowsim.datastructures;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Information about individual flow.
 */
public class Flow implements Comparable<Flow> {
  static AtomicInteger nextId = new AtomicInteger();
  private int id;

  public final MapTask mapper;
  public final ReduceTask reducer;
//  private final double totalBytes;
  public double totalBytes; //csqs
  public double sentBytes; //csqs

  public double bytesRemaining;
  public double currentBps;
  public double alphaBps;//csqs
  public boolean consideredAlready;
  public int flowState;//csqs

  //csqs
  public int packetsRemaining;
  public int packetsNextToSend;

  public double dctcp_g;
  public double dctcp_alpha;
  public double ecn_frac;

  public int bbr_bandwidth_phrase;
  public int bbr_rtt_phrase;

  /**
   * Constructor for Flow.
   * 
   * @param mapper
   *          flow source.
   * @param reducer
   *          flow destination.
   * @param totalBytes
   *          size in bytes.
   */
  public Flow(MapTask mapper, ReduceTask reducer, double totalBytes) {
    this.id = nextId.incrementAndGet();

    this.mapper = mapper;
    this.reducer = reducer;
    this.totalBytes = totalBytes;

    this.bytesRemaining = totalBytes;//csqs
    this.sentBytes = 0.0;
    this.currentBps = 0.0;
    this.alphaBps = 0.0;
    this.consideredAlready = false;
    this.flowState = 0;//csqs

    //csqs
    this.packetsRemaining = (int) (totalBytes / 1024.0);
    this.packetsNextToSend = 1;

    this.dctcp_g = 0.625;
    this.dctcp_alpha = 0;
    this.ecn_frac = 0;

    this.bbr_bandwidth_phrase = 2;
    this.bbr_rtt_phrase = 1;
  }

  /**
   * For the Comparable interface.
   */
  public int compareTo(Flow arg0) {
    return id - arg0.id;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "FLOW-" + mapper + "-->" + reducer + " | " + bytesRemaining;
  }

  /**
   * Getter for totalBytes
   * 
   * @return flow size in bytes.
   */
  public double getFlowSize() {
    return totalBytes;
  }
}
