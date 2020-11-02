package com.github.saleco.bank.balance;

public class Transaction {

  private String client;
  private Integer amount;
  private String time;

  public String getClient() {
    return client;
  }

  public void setClient(String client) {
    this.client = client;
  }

  public Integer getAmount() {
    return amount;
  }

  public void setAmount(Integer amount) {
    this.amount = amount;
  }

  public String getTime() {
    return time;
  }

  public void setTime(String time) {
    this.time = time;
  }
}
