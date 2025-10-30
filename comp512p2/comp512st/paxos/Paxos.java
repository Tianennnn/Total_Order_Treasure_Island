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

    int promisedBID = -1;
    int proposedBID = -1;
    Object proposedVal;
    Object originalProposedVal;

    int acceptedBID = -1;
    Object acceptedVal = null;

    Map<Integer, Object> prevAcceptedIDVals = new HashMap<>();
    List<String> promisedProcess = new ArrayList<String>();

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
        List<String> validTypes = Arrays.asList("PROPOSE", "PROMISE", "REFUSE", "ACCEPT", "ACCEPTED", "DECIDE");

        int ballotID;
        int prevAcceptedBID = -1;
        Object prevAcceptedVal;

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

    }

    

    private void proposeToAll(int ballotID) {
        PaxosMessage msg = new PaxosMessage("PROPOSE");
        msg.setBallotID(ballotID);
        gcl.broadcastMsg(msg);
    }

    private boolean hasMajorityPromised(){
        long start = System.currentTimeMillis();
        // allow 1 second to hear back the promises
        long timeout = 1000;
        while (System.currentTimeMillis() - start < timeout) {
            // return true as soon as reach the majority
            if (promisedProcess.size() > allGroupProcesses.size()/2) {
                return true;
            }
        }
        // if time out
        return false;
    }

    public void broadcastTOMsg_temp(Object val) {
        ballotIDCounter ++;
        int ballotID = ballotIDCounter;
        proposedBID = ballotID;
        proposedVal = val;
        proposeToAll(ballotID);
        
        if ( !hasMajorityPromised() ){
            // start over
            prevAcceptedIDVals = new HashMap<>();
            promisedProcess = new ArrayList<String>();
            broadcastTOMsg_temp(val);
        }
        else{
            // if some processes has previously accepted other values
            if(prevAcceptedIDVals.size()>0){
                // get the previously accepted value with the highest ballot ID
                Integer highestID = -1;
                for (Integer id : prevAcceptedIDVals.keySet()) {
                    if (id > highestID) {
                        highestID = id;
                    }
                }
                // propose the value with the highest ballot ID instead, for the rest of the PAXOS round
                originalProposedVal = proposedVal;      // save the original proposed value
                proposedVal = prevAcceptedIDVals.get(highestID);
            }
        }


    }

    public Object acceptTOMsg_temp() throws InterruptedException {
        Object confirmedVal = null;

        // keeps listening for messages
        while(true){
            GCMessage gcmsg = gcl.readGCMessage();
    
            String sender = gcmsg.senderProcess;
            PaxosMessage msg = (PaxosMessage) gcmsg.val;

            if (msg.type.equals("PROPOSE")){
                PaxosMessage promiseMsg;
                int poposedBID = msg.ballotID;
                if (poposedBID < promisedBID) {
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
                    promisedBID = poposedBID;
                }
                // return the message to the sender
                gcl.sendMsg(promiseMsg, sender);
            }

            else if (msg.type.equals("PROMISE")) {
                int promisedBID = msg.ballotID;
                if(promisedBID == proposedBID){
                    promisedProcess.add(sender);
                    // if previously accepted other value
                    if (msg.prevAcceptedBID != -1 && msg.prevAcceptedVal != null) {
                        prevAcceptedIDVals.put(msg.prevAcceptedBID, msg.prevAcceptedVal);
                    }
                }

            }

            else if (msg.type.equals("CONFIRM")) {
                confirmedVal = null;
                break;
            }
        }

        return confirmedVal;
    }
}

