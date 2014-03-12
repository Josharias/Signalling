package org.terasology.signalling.componentSystem;

import org.terasology.blockNetwork.SidedLocationNetworkNode;
import org.terasology.math.Vector3i;

/**
 * @author Marcin Sciesinski <marcins78@gmail.com>
 */
public class SignalNetworkNode extends SidedLocationNetworkNode {
    public enum Type { PRODUCER, CONSUMER, CONDUCTOR };

    private Type type;

    public SignalNetworkNode(Vector3i location, byte connectionSides, Type type) {
        super(location, connectionSides);
        this.type = type;
    }

    public Type getType() {
        return type;
    }
}
