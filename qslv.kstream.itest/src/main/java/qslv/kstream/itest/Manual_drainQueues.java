package qslv.kstream.itest;

import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.BalanceLog;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.EnableQuickSilver;

@SpringBootTest
@EnableQuickSilver
class Manual_drainQueues {

	@Autowired
	ConfigProperties config;
	@Autowired
	KafkaProducerDao kafkaProducerDao;

	// Response Queue
	@Autowired ArrayBlockingQueue<ResponseMessage<PostingRequest,PostingResponse>> responseExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> reservationMatchExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<WorkflowMessage>> transactionProcessorExchangeQueue;
	@Autowired ArrayBlockingQueue<LoggedTransaction> reservationByUuidExchangeQueue;
	@Autowired ArrayBlockingQueue<TraceableMessage<LoggedTransaction>> loggedTransactionExchangeQueue;
	@Autowired ArrayBlockingQueue<BalanceLog> balanceLogExchangeQueue;

	/**
	 * This is stupid, but I just need a quick way to make sure the test consumer group has caught up
	 * to the last message in the topic. There appears to be a timing issue, where the test application
	 * executes too fast and doesnt leave enough time for the queues to be drained.
	 * 
	 * @throws Exception
	 */
	
	@Test
	void drain_queues_and_hang() throws Exception {
		while (true ) {
			responseExchangeQueue.take();
		}
	}

}
