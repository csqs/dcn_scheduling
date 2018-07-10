package coflowsim.datastructures;

import java.util.Vector;

import coflowsim.datastructures.Flow;

/**
 * Information of each Rack.
 */
public class RackNode {
  public int rackID;

  public Vector<Flow> detectFlows;

  /**
   * Constructor for Rack.
   * 
   * @param machineID
   *          ID of this Rack.
   */
  public RackNode(int rackID) {
    this.rackID = rackID;
    this.detectFlows = new Vector<Flow>();
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return "Rack-" + rackID;
  }
}
