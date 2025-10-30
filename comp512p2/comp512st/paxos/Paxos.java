package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.logging.*;
import java.net.UnknownHostException;


// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.
// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos
{
    List<String> allGroupProcesses = new ArrayList<String>();
    
	GCL gcl;
	FailCheck failCheck;

    static int ballotIDCounter = 0;
    static Queue<Integer> queueTO = new PriorityQueue<>();

    int promisingBID = -1;
    int proposingBID = -1;
    Object proposingVal;
    Object originalProposingVal;

    int acceptedBID = -1;
    Object acceptedVal = null;

    Map<Integer, Object> prevAcceptedIDVals = new HashMap<>();
    List<String> promisedProcesses = new ArrayList<String>();
    
    List<String> acceptAckProcesses = new ArrayList<String>();

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck) throws IOException, UnknownHostException
	{
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger);

        this.allGroupProcesses = Arrays.asList(allGroupProcesses);
	}

	// This is what the application layer is going to call to send a message/value, such as the player and the move
	public void broadcastTOMsg(Object val)
	{
		// This is just a place holder.
		// Extend this to build whatever Paxos logic you need to make sure the messaging system is total order.
		// Here you will have to ensure that the CALL BLOCKS, and is returned ONLY when a majority (and immediately upon majority) of processes have accepted the value.
		gcl.broadcastMsg(val);
	}

	// This is what the application layer is calling to figure out what is the next message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in the same order.
	public Object acceptTOMsg() throws InterruptedException
	{
		// This is just a place holder.
		GCMessage gcmsg = gcl.readGCMessage();
		return gcmsg.val;
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos()
	{
		gcl.shutdownGCL();
	}



    private class PaxosMessage {
        String type;
        List<String> validTypes = Arrays.asList("PROPOSE", "PROMISE", "REFUSE", "ACCEPT", "DENY","ACCEPT_ACK", "CONFIRM");

        int ballotID;
        int prevAcceptedBID = -1;
        Object prevAcceptedVal;

        Object acceptVal;

        public PaxosMessage(String type){
            if (validTypes.contains(type.toUpperCase())){
                this.type = type.toUpperCase();
            }
            else{
                System.out.print("Invalid Paxos message. The message must be one of the types:");
                System.out.println(validTypes);
            }
        }

        public void setBallotID(int ballotID){
            this.ballotID = ballotID;
        }

        public void setPrevAcceptedBID(int prevAcceptedBID) {
            this.prevAcceptedBID = prevAcceptedBID;
        }

        public void setPrevAcceptedVal(Object prevAcceptedVal) {
            this.prevAcceptedVal = prevAcceptedVal;
        }

        public void setAcceptVal(Object val) {
            this.acceptVal = val;
        }
    }



    private void broadcastAcceptToAll(int ballotID, Object val){
        PaxosMessage msg = new PaxosMessage("ACCEPT");
        msg.setBallotID(ballotID);
        msg.setAcceptVal(val);

        // for later checking whether majority processes has promised
        acceptAckProcesses = new ArrayList<String>();

        gcl.broadcastMsg(msg);
    }    

    private void proposeToAll(int ballotID) {
        PaxosMessage msg = new PaxosMessage("PROPOSE");
        msg.setBallotID(ballotID);

        // for tracking the processes that has previously accepted values from other proposers
        prevAcceptedIDVals = new HashMap<>();
        // for later checking whether majority processes has promised
        promisedProcesses = new ArrayList<String>();
        
        gcl.broadcastMsg(msg);
    }

    private boolean hasMajorityPromised(){
        long start = System.currentTimeMillis();
        // allow 1 second to hear back the promises
        long timeout = 1000;
        while (System.currentTimeMillis() - start < timeout) {
            // return true as soon as reach the majority
            if (promisedProcesses.size() > allGroupProcesses.size()/2) {
                return true;
            }
        }
        // if time out
        return false;
    }


    public void broadcastTOMsg_temp(Object val) {
        ballotIDCounter ++;
        int ballotID = ballotIDCounter;
        proposingBID = ballotID;
        proposingVal = val;
        proposeToAll(ballotID);
        
        if ( !hasMajorityPromised() ){
            // start over
            broadcastTOMsg_temp(val);
            return;
        }
        
        // if some processes has previously accepted other values
        if(prevAcceptedIDVals.size()>0){
            // get the previously accepted value with the highest ballot ID
            Integer highestBID = -1;
            for (Integer id : prevAcceptedIDVals.keySet()) {
                if (id > highestBID) {
                    highestBID = id;
                }
            }
            // propose the value with the highest ballot ID instead, for the rest of the PAXOS round
            Object valForHighestBID = prevAcceptedIDVals.get(highestBID);
            if ( !valForHighestBID.equals(proposingVal) ){
                // save the original proposed value
                originalProposingVal = proposingVal;
            }
            proposingVal = valForHighestBID;
        }

        broadcastAcceptToAll(proposingBID, proposingVal);
        

    }

    public Object acceptTOMsg_temp() throws InterruptedException {
        Object confirmedVal = null;

        // keeps listening for messages
        while(true){
            GCMessage gcmsg = gcl.readGCMessage();
    
            String sender = gcmsg.senderProcess;
            PaxosMessage msg = (PaxosMessage) gcmsg.val;

            if (msg.type.equals("PROPOSE")){
                int proposedBID = msg.ballotID;

                PaxosMessage promiseMsg;
                if (proposedBID < promisingBID) {
                    promiseMsg = new PaxosMessage("REFUSE");
                }
                else{
                    promiseMsg = new PaxosMessage("PROMISE");
                    // include the previously accepted ballot id and value in the return message
                    if(acceptedBID != -1 && acceptedVal != null){
                        promiseMsg.setPrevAcceptedBID(acceptedBID);
                        promiseMsg.setPrevAcceptedVal(acceptedVal);
                    }
                    // promise to the new bollot id
                    promisingBID = proposedBID;
                    promiseMsg.setBallotID(promisingBID);
                }
                // reply the promise message to the sender
                gcl.sendMsg(promiseMsg, sender);
            }

            else if (msg.type.equals("PROMISE")) {
                int promisedBID = msg.ballotID;
                if(promisedBID == proposingBID){
                    promisedProcesses.add(sender);
                    // if previously accepted other value
                    if (msg.prevAcceptedBID != -1 && msg.prevAcceptedVal != null) {
                        prevAcceptedIDVals.put(msg.prevAcceptedBID, msg.prevAcceptedVal);
                    }
                }
            }

            else if (msg.type.equals("ACCEPT")) {
                int proposedBID = msg.ballotID;
                Object proposedVal = msg.acceptVal;

                PaxosMessage acceptAckMsg;
                if(proposedBID < promisingBID){
                    acceptAckMsg = new PaxosMessage("DENY");
                }
                else{
                    acceptAckMsg = new PaxosMessage("ACCEPT_ACK");
                    // accept the value
                    acceptedVal = proposedVal;
                    acceptedBID = proposedBID;
                    acceptAckMsg.setBallotID(proposedBID);
                }

                // reply the accept_ack message to the sender
                gcl.sendMsg(acceptAckMsg, sender);
            }


            else if (msg.type.equals("CONFIRM")) {
                confirmedVal = null;
                break;
            }
        }

        return confirmedVal;
    }
}

