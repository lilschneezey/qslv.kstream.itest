package qslv.kstream.itest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaTestApplication {
	public static void main(String[] args) {
		SpringApplication.run(KafkaTestApplication.class, args);
	}
}
/*
 * reservation.
 * setup account balance and account status
 * call reservation.
 * verify account balance and status
 * 1. normal
 * 2. NSF - no OD
 * 3. bad status
 * 4. NSF - 1. OD
 * 5. NSF - 1. OD expired
 * 6. NSF - 1. OD bad status
 * 7. NSF - 3. OD 2 more NSF
 *  
 */
