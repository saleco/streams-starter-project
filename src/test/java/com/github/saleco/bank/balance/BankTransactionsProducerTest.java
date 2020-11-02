package com.github.saleco.bank.balance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.saleco.bank.balance.BankTransactionsProducer;
import com.github.saleco.bank.balance.Transaction;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.*;

public class BankTransactionsProducerTest {

  @Test
  public void newRandomTransactionsTest() throws IOException {
    String result = BankTransactionsProducer.createRandomTransaction(new Random().nextInt(6));
    assertNotNull(result);

    ObjectMapper objectMapper = new ObjectMapper();
    Transaction transaction = objectMapper.readValue(result, Transaction.class);
    assertNotNull(transaction);
    assertNotNull(transaction.getClient());
    assertFalse(transaction.getClient().isEmpty());
    assertNotNull(transaction.getTime());
    assertFalse(transaction.getTime().isEmpty());
    assertTrue("Amount should be less than 100", transaction.getAmount() < 100);
  }
}
