package com.intel.picklepot;

import java.io.Serializable;

public class Order implements Serializable{
  private int orderKey;
  private int custKey;
  private String orderStatus;
  //skip price column
  private String date;
  private String orderPriority;
  private String clerk;
  private int shipProprity;
  //skip comment column

  public Order(String line) {
    String[] values = line.split("\\|");
    orderKey = Integer.parseInt(values[0]);
    custKey = Integer.parseInt(values[1]);
    orderStatus = values[2];
    date = values[4];
    orderPriority = values[5];
    clerk = values[6];
    shipProprity = Integer.parseInt(values[7]);
  }

  //default contructor to enable kryo serialization
  public Order() {}

  @Override
  public boolean equals(Object object) {
    if(object instanceof Order) {
      Order o = (Order)object;
      return orderKey == o.orderKey
          && custKey == o.custKey
          && orderStatus.equals(o.orderStatus)
          && date.equals(o.date)
          && orderPriority.equals(o.orderPriority)
          && clerk.equals(o.clerk)
          && shipProprity == o.shipProprity;
    }
    return false;
  }
}
