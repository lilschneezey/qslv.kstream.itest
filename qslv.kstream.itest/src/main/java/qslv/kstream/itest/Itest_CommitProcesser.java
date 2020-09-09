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
import qslv.kstream.CancelReservationRequest;
import qslv.kstream.CommitReservationRequest;
import qslv.kstream.LoggedTransaction;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.ReservationRequest;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.EnableQuickSilver;
import qslv.util.Random;

@SpringBootTest
@EnableQuickSilver
class Itest_CommitProcesser {

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

	//TODO: basic OD
	//TODO: basic validation conditions, OD variations
	//TODO: balanceLog validation
	
	@Test
	void testCommit_success_negativeRemaining() throws Exception {
		testCommit_success(9999L, -3333L, -5000L);
	}
	
	@Test
	void testCommit_success_positiveRemaining()throws Exception  {
		testCommit_success(9999L, -3333L, -1000L);
	}
	
	@Test
	void testCommit_success_zeroRemaining() throws Exception {
		testCommit_success(9999L, -3333L, -3333L);
	}

	void testCommit_success(long starting_balance, long transaction_amount, long commit_amount) throws Exception {
		drain_queues();

		long expected_balance = starting_balance + commit_amount;
		long commit_difference = commit_amount - transaction_amount;
		
		// - setup --------------------
		Account account = randomAccount(true);
		kafkaProducerDao.produceAccount(account);
		resetBalance(account.getAccountNumber(), starting_balance);
		
		// Create Request
		LoggedTransaction reservation = makeReservation( account, transaction_amount);

		// - prepare --------------------
		CommitReservationRequest request = setupCommitRequest(account, reservation.getTransactionUuid(), commit_amount);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(request));

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
		assertNotNull(response.getRequest().getCommitReservationRequest());
		verifyRequest(request, response.getRequest().getCommitReservationRequest());

		assertNotNull(response.getResponse());

		assertEquals(1, response.getResponse().getTransactions().size());

		LoggedTransaction commit = response.getResponse().getTransactions().get(0);
		assertEquals(request.getAccountNumber(), commit.getAccountNumber());
		assertEquals(request.getRequestUuid(), commit.getRequestUuid());
		assertEquals(reservation.getTransactionUuid(), commit.getReservationUuid());
		assertEquals(expected_balance, commit.getRunningBalanceAmount());
		assertEquals(commit_difference, commit.getTransactionAmount());
		assertEquals(request.getJsonMetaData(), commit.getTransactionMetaDataJson());
		assertNotNull(commit.getTransactionTime());
		assertEquals(LoggedTransaction.RESERVATION_COMMIT, commit.getTransactionTypeCode());
		assertNotNull(commit.getTransactionUuid());

		TraceableMessage<LoggedTransaction> tracedTransaction = loggedTransactionExchangeQueue.take();
		verifyTraceableMessage(traceable, tracedTransaction);
		verifyTransactions(commit, tracedTransaction.getPayload());

		reservationMatchExchangeQueue.take();
		transactionProcessorExchangeQueue.take();
		reservationByUuidExchangeQueue.take();
	}
	
	LoggedTransaction makeReservation(Account account, long amount) throws Exception {
		ReservationRequest reservationRequest = setupReservationRequest(account, amount);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(reservationRequest));

		kafkaProducerDao.produceRequestMessage(traceable);

		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();
		assertEquals(ResponseMessage.SUCCESS, response.getStatus());
		
		TraceableMessage<LoggedTransaction> transaction = loggedTransactionExchangeQueue.take();
		assertEquals(LoggedTransaction.RESERVATION, transaction.getPayload().getTransactionTypeCode());

		transactionProcessorExchangeQueue.take();
		reservationByUuidExchangeQueue.take();	

		return transaction.getPayload();
	}
	
	@Test
	void testCommit_noMatch() throws Exception {
		drain_queues();

		long starting_balance = 9999L;

		// - setup --------------------
		Account account = randomAccount(true);
		kafkaProducerDao.produceAccount(account);
		resetBalance(account.getAccountNumber(), starting_balance);
		
		// - prepare --------------------
		CommitReservationRequest request = setupCommitRequest(account, UUID.randomUUID(), -99L);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(request));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.CONFLICT, response.getStatus());
		assertTrue(response.getErrorMessage().contains("No match"));

		assertNotNull(response.getRequest());
		assertNotNull(response.getRequest().getCommitReservationRequest());
		verifyRequest(request, response.getRequest().getCommitReservationRequest());

		assertNotNull(response.getResponse());

		assertEquals(0, response.getResponse().getTransactions().size());
		
		reservationMatchExchangeQueue.take();
		transactionProcessorExchangeQueue.take();
	}
	
	@Test
	void alreadyCanceled() throws Exception {
		drain_queues();

		long starting_balance = 9999L;
		long transaction_amount = -4444L;

		// - setup --------------------
		Account account = randomAccount(true);
		kafkaProducerDao.produceAccount(account);
		resetBalance(account.getAccountNumber(), starting_balance);
		
		// Create Request
		LoggedTransaction reservation = makeReservation( account, transaction_amount);

		// - prepare --------------------
		CancelReservationRequest request = setupCancelRequest(account, reservation.getTransactionUuid());
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(request));

		// - execute Cancel First------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		reservationMatchExchangeQueue.take();
		transactionProcessorExchangeQueue.take();
		reservationByUuidExchangeQueue.take();
		loggedTransactionExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.SUCCESS, response.getStatus());
		
		// Now execute Commit
		// - prepare --------------------
		CommitReservationRequest secondRequest = setupCommitRequest(account, reservation.getTransactionUuid(), transaction_amount);
		TraceableMessage<PostingRequest> secondTraceable = setupTraceable(new PostingRequest(secondRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(secondTraceable);
		ResponseMessage<PostingRequest, PostingResponse> secondResponse = responseExchangeQueue.take();
		
		assertNotNull(secondResponse);
		verifyTraceableMessage(secondTraceable, secondResponse);
		assertNotNull(secondResponse.getMessageCompletionTime());
		assertEquals(ResponseMessage.CONFLICT, secondResponse.getStatus());
		assertTrue(secondResponse.getErrorMessage().contains("No match"));

		assertNotNull(secondResponse.getRequest());
		assertNotNull(secondResponse.getRequest().getCommitReservationRequest());
		verifyRequest(secondRequest, secondResponse.getRequest().getCommitReservationRequest());

		assertNotNull(secondResponse.getResponse());

		assertEquals(0, secondResponse.getResponse().getTransactions().size());
		
		reservationMatchExchangeQueue.take();
		transactionProcessorExchangeQueue.take();
	}
	
	@Test
	void alreadyCommitted() throws Exception {
		drain_queues();

		long starting_balance = 9999L;
		long transaction_amount = -4444L;

		// - setup --------------------
		Account account = randomAccount(true);
		kafkaProducerDao.produceAccount(account);
		resetBalance(account.getAccountNumber(), starting_balance);
		
		// Create Request
		LoggedTransaction reservation = makeReservation( account, transaction_amount);

		// - prepare --------------------
		CommitReservationRequest request = setupCommitRequest(account, reservation.getTransactionUuid(), transaction_amount);
		TraceableMessage<PostingRequest> traceable = setupTraceable(new PostingRequest(request));

		// - execute Commit First ------------------
		kafkaProducerDao.produceRequestMessage(traceable);
		ResponseMessage<PostingRequest, PostingResponse> response = responseExchangeQueue.take();

		// - verify ------------------
		assertNotNull(response);
		verifyTraceableMessage(traceable, response);
		assertNotNull(response.getMessageCompletionTime());
		assertEquals(ResponseMessage.SUCCESS, response.getStatus());
		
		// Now Do it again
		// - prepare --------------------
		CommitReservationRequest secondRequest = setupCommitRequest(account, reservation.getTransactionUuid(), transaction_amount);
		TraceableMessage<PostingRequest> secondTraceable = setupTraceable(new PostingRequest(secondRequest));

		// - execute ------------------
		kafkaProducerDao.produceRequestMessage(secondTraceable);
		ResponseMessage<PostingRequest, PostingResponse> secondResponse = responseExchangeQueue.take();
		
		assertNotNull(secondResponse);
		verifyTraceableMessage(secondTraceable, secondResponse);
		assertNotNull(secondResponse.getMessageCompletionTime());
		assertEquals(ResponseMessage.CONFLICT, secondResponse.getStatus());
		assertTrue(secondResponse.getErrorMessage().contains("No match"));

		assertNotNull(secondResponse.getRequest());
		assertNotNull(secondResponse.getRequest().getCommitReservationRequest());
		verifyRequest(secondRequest, secondResponse.getRequest().getCommitReservationRequest());

		assertNotNull(secondResponse.getResponse());

		assertEquals(0, secondResponse.getResponse().getTransactions().size());
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

	ReservationRequest setupReservationRequest(Account account, long amount) {
		ReservationRequest request = new ReservationRequest();
		request.setRequestUuid(UUID.randomUUID());
		request.setAccountNumber(account.getAccountNumber());
		request.setDebitCardNumber(null);
		request.setTransactionAmount(amount);
		request.setJsonMetaData(JSON_DATA);
		return request;
	}
	
	CancelReservationRequest setupCancelRequest(Account account, UUID reservationUuid) {
		CancelReservationRequest request = new CancelReservationRequest();
		request.setRequestUuid(UUID.randomUUID());
		request.setAccountNumber(account.getAccountNumber());
		request.setJsonMetaData(JSON_DATA);
		request.setReservationUuid(reservationUuid);
		return request;
	}

	CommitReservationRequest setupCommitRequest(Account account, UUID reservationUuid, long amount) {
		CommitReservationRequest request = new CommitReservationRequest();
		request.setRequestUuid(UUID.randomUUID());
		request.setAccountNumber(account.getAccountNumber());
		request.setJsonMetaData(JSON_DATA);
		request.setReservationUuid(reservationUuid);
		request.setTransactionAmount(amount);
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

	void verifyRequest(CommitReservationRequest expected, CommitReservationRequest actual) {
		assertEquals(expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals(expected.getReservationUuid(), actual.getReservationUuid());
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
