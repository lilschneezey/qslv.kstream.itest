package qslv.kstream.itest;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.BalanceLog;
import qslv.data.OverdraftInstruction;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.TransactionRequest;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.EnableQuickSilver;
import qslv.util.Random;

@SpringBootTest
@EnableQuickSilver
class Itest_TransactionProcesser {

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


	public static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public static String VALID_STATUS = "EF";
	public static String INVALID_STATUS = "CL";
	public static String JSON_DATA = "{\"value\": 234934}";

	public void drain_queues() {
		responseExchangeQueue.clear();
		reservationMatchExchangeQueue.clear();
		transactionProcessorExchangeQueue.clear();
		reservationByUuidExchangeQueue.clear();
		loggedTransactionExchangeQueue.clear();
		balanceLogExchangeQueue.clear();
	}

	//TODO: basic validation conditions, OD variations
	//TODO: balanceLog validation
	
	@Test
	void testTransaction_negativeAmount() throws Exception {
		testTransaction_Amount(9999L, -4444L);
	}
	@Test
	void testTransaction_positiveAmount() throws Exception {
		testTransaction_Amount(9999L, 4444L);
	}
	
	void testTransaction_Amount(long starting_balance, long transaction_amount) throws Exception {
		drain_queues();

		long expected_balance = starting_balance + transaction_amount;

		// - setup --------------------
		Account account = randomAccount(true);
		kafkaProducerDao.produceAccount(account);
		resetBalance(account.getAccountNumber(), starting_balance);

		// - prepare --------------------
		TransactionRequest transactionRequest = setupRequest(account, transaction_amount);
		transactionRequest.setAuthorizeAgainstBalance(true);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(transactionRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.SUCCESS, response.getStatus());
		assertNull(response.getErrorMessage());

		assertNotNull(response.getRequest());
		assertNotNull(response.getRequest().getTransactionRequest());
		verifyRequest(transactionRequest, response.getRequest().getTransactionRequest());

		assertNotNull(response.getResponse());

		assertEquals(1, response.getResponse().getTransactions().size());

		LoggedTransaction transaction = response.getResponse().getTransactions().get(0);
		assertEquals(transactionRequest.getAccountNumber(), transaction.getAccountNumber());
		assertNull(transactionRequest.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals(transactionRequest.getRequestUuid(), transaction.getRequestUuid());
		assertNull(transaction.getReservationUuid());
		assertEquals(expected_balance, transaction.getRunningBalanceAmount());
		assertEquals(transaction_amount, transaction.getTransactionAmount());
		assertEquals(transactionRequest.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull(transaction.getTransactionTime());
		assertEquals(LoggedTransaction.NORMAL, transaction.getTransactionTypeCode());
		assertNotNull(transaction.getTransactionUuid());

		TraceableMessage<LoggedTransaction> tracedTransaction = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransaction);
		verifyTransactions(transaction, tracedTransaction.getPayload());
		
		// clear queues
		transactionProcessorExchangeQueue.take();
		//balanceLogExchangeQueue.take();
	}

	@Test
	void testTransaction_NSF() throws Exception {
		drain_queues();

		long starting_balance = 3333L;
		long transaction_amount = -4444L;

		// - setup --------------------
		Account account = randomAccount(true);
		kafkaProducerDao.produceAccount(account);
		resetBalance(account.getAccountNumber(), starting_balance);

		// - prepare --------------------
		TransactionRequest transactionRequest = setupRequest(account, transaction_amount);
		transactionRequest.setAuthorizeAgainstBalance(true);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(transactionRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.INSUFFICIENT_FUNDS, response.getStatus());
		assertTrue(response.getErrorMessage().contains("Insufficient funds"));

		assertNotNull(response.getRequest());
		assertNotNull(response.getRequest().getTransactionRequest());
		verifyRequest(transactionRequest, response.getRequest().getTransactionRequest());

		assertNotNull(response.getResponse());

		assertEquals(1, response.getResponse().getTransactions().size());

		LoggedTransaction transaction = response.getResponse().getTransactions().get(0);
		assertEquals(transactionRequest.getAccountNumber(), transaction.getAccountNumber());
		assertNull(transactionRequest.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals(transactionRequest.getRequestUuid(), transaction.getRequestUuid());
		assertNull(transaction.getReservationUuid());
		assertEquals(starting_balance, transaction.getRunningBalanceAmount());
		assertEquals(transaction_amount, transaction.getTransactionAmount());
		assertEquals(transactionRequest.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull(transaction.getTransactionTime());
		assertEquals(LoggedTransaction.REJECTED_TRANSACTION, transaction.getTransactionTypeCode());
		assertNotNull(transaction.getTransactionUuid());

		TraceableMessage<LoggedTransaction> tracedTransaction = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransaction);
		verifyTransactions(transaction, tracedTransaction.getPayload());
		
		transactionProcessorExchangeQueue.take();

	}
	
	@Test
	void testTransaction_ODprotection() throws Exception {
		drain_queues();

		long starting_balance = 3333L;
		long startingODbalance = 9999L;
		long transaction_amount = -4444L;
		long intermediary_balance = starting_balance + Math.abs(transaction_amount);
		long expectedODbalance = startingODbalance + transaction_amount;

		// - setup --------------------
		Account account = randomAccount(true);
		account.setProtectAgainstOverdraft(true);
		OverdraftInstruction overdraft = randomOverdraft(account.getAccountNumber(), true, true);
		kafkaProducerDao.produceAccount(account);
		kafkaProducerDao.produceOverdraft(overdraft);
		resetBalance(account.getAccountNumber(), starting_balance);
		resetBalance(overdraft.getOverdraftAccount().getAccountNumber(), startingODbalance);

		// - prepare --------------------
		TransactionRequest transactionRequest = setupRequest(account, transaction_amount);
		transactionRequest.setProtectAgainstOverdraft(true);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(transactionRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.SUCCESS, response.getStatus());
		assertNull(response.getErrorMessage());

		assertNotNull(response.getRequest());
		assertNotNull(response.getRequest().getTransactionRequest());
		verifyRequest(transactionRequest, response.getRequest().getTransactionRequest());

		assertNotNull(response.getResponse());

		assertEquals(4, response.getResponse().getTransactions().size());

		LoggedTransaction rejection = response.getResponse().getTransactions().get(0);
		assertEquals(transactionRequest.getAccountNumber(), rejection.getAccountNumber());
		assertNull(transactionRequest.getDebitCardNumber(), rejection.getDebitCardNumber());
		assertEquals(transactionRequest.getRequestUuid(), rejection.getRequestUuid());
		assertNull(rejection.getReservationUuid());
		assertEquals(starting_balance, rejection.getRunningBalanceAmount());
		assertEquals(transaction_amount, rejection.getTransactionAmount());
		assertEquals(transactionRequest.getJsonMetaData(), rejection.getTransactionMetaDataJson());
		assertNotNull(rejection.getTransactionTime());
		assertEquals(LoggedTransaction.REJECTED_TRANSACTION, rejection.getTransactionTypeCode());
		assertNotNull(rejection.getTransactionUuid());
		
		LoggedTransaction transferFrom = response.getResponse().getTransactions().get(1);
		assertEquals(overdraft.getOverdraftAccount().getAccountNumber(), transferFrom.getAccountNumber());
		assertNull(transactionRequest.getDebitCardNumber(), transferFrom.getDebitCardNumber());
		assertEquals(transactionRequest.getRequestUuid(), transferFrom.getRequestUuid());
		assertNull(transferFrom.getReservationUuid());
		assertEquals(expectedODbalance, transferFrom.getRunningBalanceAmount());
		assertEquals(transaction_amount, transferFrom.getTransactionAmount());
		assertEquals(transactionRequest.getJsonMetaData(), transferFrom.getTransactionMetaDataJson());
		assertNotNull(transferFrom.getTransactionTime());
		assertEquals(LoggedTransaction.TRANSFER_FROM, transferFrom.getTransactionTypeCode());
		assertNotNull(transferFrom.getTransactionUuid());
		
		LoggedTransaction transferTo = response.getResponse().getTransactions().get(2);
		assertEquals(account.getAccountNumber(), transferTo.getAccountNumber());
		assertNull(transactionRequest.getDebitCardNumber(), transferTo.getDebitCardNumber());
		assertEquals(transactionRequest.getRequestUuid(), transferTo.getRequestUuid());
		assertNull(transferTo.getReservationUuid());
		assertEquals(intermediary_balance, transferTo.getRunningBalanceAmount());
		assertEquals(Math.abs(transaction_amount), transferTo.getTransactionAmount());
		assertEquals(transactionRequest.getJsonMetaData(), transferTo.getTransactionMetaDataJson());
		assertNotNull(transferTo.getTransactionTime());
		assertEquals(LoggedTransaction.TRANSFER_TO, transferTo.getTransactionTypeCode());
		assertNotNull(transferTo.getTransactionUuid());

		LoggedTransaction transaction = response.getResponse().getTransactions().get(3);
		assertEquals(account.getAccountNumber(), transaction.getAccountNumber());
		assertNull(transactionRequest.getDebitCardNumber(), transaction.getDebitCardNumber());
		assertEquals(transactionRequest.getRequestUuid(), transaction.getRequestUuid());
		assertNull(transaction.getReservationUuid());
		assertEquals(starting_balance, transaction.getRunningBalanceAmount());
		assertEquals(transaction_amount, transaction.getTransactionAmount());
		assertEquals(transactionRequest.getJsonMetaData(), transaction.getTransactionMetaDataJson());
		assertNotNull(transaction.getTransactionTime());
		assertEquals(LoggedTransaction.NORMAL, transaction.getTransactionTypeCode());
		assertNotNull(transaction.getTransactionUuid());

		TraceableMessage<LoggedTransaction> tracedRejection = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedRejection);
		verifyTransactions(rejection, tracedRejection.getPayload());
		
		TraceableMessage<LoggedTransaction> tracedTransferFrom = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransferFrom);
		verifyTransactions(transferFrom, tracedTransferFrom.getPayload());
		
		TraceableMessage<LoggedTransaction> tracedTransferTo = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransferTo);
		verifyTransactions(transferTo, tracedTransferTo.getPayload());

		TraceableMessage<LoggedTransaction> tracedTransaction = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransaction);
		verifyTransactions(transaction, tracedTransaction.getPayload());
		
		transactionProcessorExchangeQueue.take();
		transactionProcessorExchangeQueue.take();
		transactionProcessorExchangeQueue.take();

	}
	
	private void resetBalance(String accountNumber, long balance) {
		BalanceLog log = new BalanceLog();
		log.setAccountNumber(accountNumber);
		log.setLastTransaction(UUID.randomUUID());
		log.setBalance(balance);
		kafkaProducerDao.produceBalanceLog(log);
	}

	Account randomAccount(boolean valid) {
		Account account = new Account();
		account.setAccountLifeCycleStatus(valid ? "EF" : "CL");
		account.setAccountNumber(Random.randomDigits(12));
		return account;
	}

	OverdraftInstruction randomOverdraft(String accountNumber, boolean valid, boolean accountValid) {
		OverdraftInstruction od = new OverdraftInstruction();
		od.setAccountNumber(accountNumber);
		od.setEffectiveStart(LocalDateTime.now().minusMonths(12));
		od.setEffectiveEnd(LocalDateTime.now().plusMonths(12));
		od.setInstructionLifecycleStatus(valid ? "EF" : "CL");
		od.setOverdraftAccount(randomAccount(accountValid));
		return od;
	}

	TransactionRequest setupRequest(Account account, long amount) {
		TransactionRequest request = new TransactionRequest();
		request.setRequestUuid(UUID.randomUUID());
		request.setAccountNumber(account.getAccountNumber());
		request.setDebitCardNumber(null);
		request.setTransactionAmount(amount);
		request.setJsonMetaData(JSON_DATA);
		request.setAuthorizeAgainstBalance(true);
		request.setProtectAgainstOverdraft(true);
		return request;
	}

	<T> TraceableMessage<T> setupTraceable(T payload) {
		TraceableMessage<T> traceable = new TraceableMessage<>();
		traceable.setProducerAit(config.getAitid());
		traceable.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		traceable.setCorrelationId("sdfsdfsdf");
		traceable.setPayload(payload);
		traceable.setMessageCreationTime(LocalDateTime.now());
		return traceable;
	}

	void verifyRequest(TransactionRequest expected, TransactionRequest actual) {
		assertEquals(expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals(expected.getDebitCardNumber(), actual.getDebitCardNumber());
		assertEquals(expected.getJsonMetaData(), actual.getJsonMetaData());
		assertEquals(expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals(expected.getTransactionAmount(), actual.getTransactionAmount());
	}

	void verifyTraceableMessage(TraceableMessage<?> expected, ResponseMessage<?, ?> actual) {
		assertEquals(expected.getBusinessTaxonomyId(), actual.getBusinessTaxonomyId());
		assertEquals(expected.getCorrelationId(), actual.getCorrelationId());
		assertEquals(expected.getMessageCreationTime(), actual.getMessageCreationTime());
		assertEquals(expected.getProducerAit(), actual.getProducerAit());
	}

	void verifyTraceableMessage(TraceableMessage<?> expected, TraceableMessage<?> actual) {
		assertEquals(expected.getBusinessTaxonomyId(), actual.getBusinessTaxonomyId());
		assertEquals(expected.getCorrelationId(), actual.getCorrelationId());
		assertEquals(expected.getMessageCreationTime(), actual.getMessageCreationTime());
		assertEquals(expected.getProducerAit(), actual.getProducerAit());
	}

	private void verifyTransactions(LoggedTransaction reservation, LoggedTransaction ttran) {
		assertEquals(reservation.getAccountNumber(), ttran.getAccountNumber());
		assertEquals(reservation.getDebitCardNumber(), ttran.getDebitCardNumber());
		assertEquals(reservation.getRequestUuid(), ttran.getRequestUuid());
		assertEquals(reservation.getReservationUuid(), ttran.getReservationUuid());
		assertEquals(reservation.getRunningBalanceAmount(), ttran.getRunningBalanceAmount());
		assertEquals(reservation.getTransactionAmount(), ttran.getTransactionAmount());
		assertEquals(reservation.getTransactionMetaDataJson(), ttran.getTransactionMetaDataJson());
		assertEquals(reservation.getTransactionTime(), ttran.getTransactionTime());
		assertEquals(reservation.getTransactionTypeCode(), ttran.getTransactionTypeCode());
		assertEquals(reservation.getTransactionUuid(), ttran.getTransactionUuid());
	}

}
