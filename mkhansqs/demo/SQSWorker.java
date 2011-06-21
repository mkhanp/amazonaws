import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Date;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.simpledb.AmazonSimpleDB;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.CreateDomainRequest;
import com.amazonaws.services.simpledb.model.DeleteAttributesRequest;
import com.amazonaws.services.simpledb.model.DeleteDomainRequest;
import com.amazonaws.services.simpledb.model.GetAttributesRequest;
import com.amazonaws.services.simpledb.model.GetAttributesResult;
import com.amazonaws.services.simpledb.model.Item;
import com.amazonaws.services.simpledb.model.ListDomainsRequest;
import com.amazonaws.services.simpledb.model.ListDomainsResult;
import com.amazonaws.services.simpledb.model.PutAttributesRequest;
import com.amazonaws.services.simpledb.model.ReplaceableAttribute;
import com.amazonaws.services.simpledb.model.SelectRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class SQSWorker {

	private static AmazonSimpleDB sdb = null;
	private static AmazonSQS sqs = null;
	public static final String META_DOMAIN_NAME = "mkhan_sqs_util";
	public static final String INPUT_MSG_QUEUE = "mkhan_test_queue";
	public static final String OUTPUT_DOMAIN_NAME = "mkhan_sqs_output";
	public static final int SleepTime = 1000;

	public static final String QueueUrl = "https://queue.amazonaws.com/417621931418/" + INPUT_MSG_QUEUE;
		
	public static AmazonSimpleDB getInstanceSDB() throws Exception {
        if ( sdb == null ) {
            sdb = new AmazonSimpleDBClient(new PropertiesCredentials(
            		SQSWorker.class.getResourceAsStream("AwsCredentials.properties")));
            sdb.setEndpoint( "https://sdb.amazonaws.com:443" );  		
        }

        return sdb;
	}
		
	public static AmazonSQS getInstanceSQS() throws Exception {
        if ( sqs == null ) {
            sqs = new AmazonSQSClient(new PropertiesCredentials(
            		SQSWorker.class.getResourceAsStream("AwsCredentials.properties")));
        }

        return sqs;
	}
	
	
	public static HashMap<String,String> getAttributesForItem( String domainName, String itemName ) throws Exception{
		GetAttributesRequest getRequest = new GetAttributesRequest( domainName, itemName ).withConsistentRead( true );
		GetAttributesResult getResult = getInstanceSDB().getAttributes( getRequest );	
		
		HashMap<String,String> attributes = new HashMap<String,String>(30);
		for ( Object attribute : getResult.getAttributes() ) {
			String name = ((Attribute)attribute).getName();
			String value = ((Attribute)attribute).getValue();
			
			attributes.put(  name, value );
		}

		return attributes;
	}
	
    public static void createAttributeForItem( String domainName, String itemName, String attributeName, String attributeValue ) throws Exception{
		List<ReplaceableAttribute> attributes = new ArrayList<ReplaceableAttribute>(1);
		attributes.add( new ReplaceableAttribute().withName( attributeName ).withValue( attributeValue ).withReplace( true ) );
		getInstanceSDB().putAttributes( new PutAttributesRequest( domainName, itemName, attributes ) );
	}
    
	public static void main(String [] args) throws Exception{
		
		System.out.println("worker starting..");
/*		pseudo logic for worker
 * 
		while(true){
			if(need_to_exit) then exit;

			msg = getNextMsgFromSQS(sqsqueue) - selects job with status new
			process(msg) - makes the simpleDB write call
			sleep(configurable_time);
		}
*/
		while(true){
			
			HashMap h = SQSWorker.getAttributesForItem(META_DOMAIN_NAME, "meta");
			int run = Integer.parseInt(h.get("worker_run").toString());
			if(run == 0){
				System.out.println("worker exiting..");
				System.exit(0);
			}
						
            // Receive messages
            System.out.println("Receiving messages from input queue.\n");
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(QueueUrl);
            List<Message> messages = getInstanceSQS().receiveMessage(receiveMessageRequest).getMessages();
            String msgbody = "";
            for (Message message : messages) {
                //System.out.println("  Message");
                //System.out.println("    MessageId:     " + message.getMessageId());
                //System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
                //System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
                System.out.println("    Body:          " + message.getBody());
                msgbody = message.getBody();
                
                System.out.println();
                
                //create processed request in amazon simple db 
                SQSWorker.createAttributeForItem(OUTPUT_DOMAIN_NAME, "processtime", msgbody, new Date().toString());
                
                //Delete a message
                System.out.println("Deleting a message.\n");
                String messageRecieptHandle = message.getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(QueueUrl, messageRecieptHandle));
                         
            }
            
            //sleep for configurable amount of time
            Thread.sleep(SleepTime);
            
           
            
		}
		
		
	}
	
	
}