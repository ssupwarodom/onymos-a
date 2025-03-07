import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

class Order {
    final String orderType;
    final String ticker;
    final int qty;
    final double price;
    AtomicReference<Order> next;

    Order(String orderType, String ticker, int qty, double price) {
        this.orderType = orderType;
        this.ticker = ticker;
        this.qty = qty;
        this.price = price;
        this.next = new AtomicReference<>(null);
    }
}

class OrderBook {
    final AtomicReference<String[]> tickerIndex;
    final AtomicReferenceArray<Order> sellOrders;
    final AtomicReferenceArray<Order> buyOrders;

    OrderBook() {
        this.tickerIndex = new AtomicReference<>(new String[1024]);
        this.sellOrders = new AtomicReferenceArray<>(1024);
        this.buyOrders = new AtomicReferenceArray<>(1024);
    }

    int getTickerIndex(String ticker) {
        while (true) {
            String[] original = this.tickerIndex.get();
            int i = 0;
            while (i < original.length && original[i] != null) {
                if (original[i] == ticker) {
                    return i;
                }
                i++;
            }

            String[] indices = Arrays.copyOf(original, original.length);
            indices[i] = ticker;
            if (this.tickerIndex.compareAndSet(original, indices)) {
                return i;
            }
        }
    }

    public void addOrder(String orderType, String ticker, int qty, double price) {
        int index = this.getTickerIndex(ticker);

        if (orderType == "Sell") {
            this.addSellOrder(index, orderType, ticker, qty, price);
        } else if (orderType == "Buy") {
            this.addBuyOrder(index, orderType, ticker, qty, price);
        }

        System.out.printf("[Add] [%s] [%s] %s @ %s\n", orderType, ticker, qty, price);
    }

    void addSellOrder(int index, String orderType, String ticker, int qty, double price) {
        
        while (true) {
            Order sellHead = this.sellOrders.get(index);
            Order newOrder = new Order(orderType, ticker, qty, price);

            if (sellHead == null) {
                if (this.sellOrders.compareAndSet(index, null, newOrder)) {
                    return;
                }
            } else if (newOrder.price <= sellHead.price){
                newOrder.next.set(sellHead);
                if (this.sellOrders.compareAndSet(index, sellHead, newOrder)) {
                    return;
                }
            } else {
                Order currentOrder = sellHead;
                Order nextOrder = sellHead.next.get();
                while (nextOrder != null ) {
                    if (newOrder.price <= nextOrder.price) {
                        break;
                    }

                    currentOrder = nextOrder;
                    nextOrder = nextOrder.next.get();
                }

                newOrder.next.set(nextOrder);
                if (currentOrder.next.compareAndSet(nextOrder, newOrder)) {
                    return;
                }
            }
        }


    }

    void addBuyOrder(int index, String orderType, String ticker, int qty, double price) {

        while (true) {
            Order buyHead = this.buyOrders.get(index);
            Order newOrder = new Order(orderType, ticker, qty, price);

            if (buyHead == null) {
                if (this.buyOrders.compareAndSet(index, null, newOrder)) {
                    return;
                }
            } else if (newOrder.price >= buyHead.price){
                newOrder.next.set(buyHead);
                if (this.buyOrders.compareAndSet(index, buyHead, newOrder)) {
                    return;
                }
            } else {
                Order currentOrder = buyHead;
                Order nextOrder = buyHead.next.get();
                while (nextOrder != null ) {
                    if (newOrder.price >= nextOrder.price) {
                        break;
                    }

                    currentOrder = nextOrder;
                    nextOrder = nextOrder.next.get();
                }

                newOrder.next.set(nextOrder);
                if (currentOrder.next.compareAndSet(nextOrder, newOrder)) {
                    return;
                }
            }
        }
    }

    // public void matchOrders() {
    //     int i = 0;
    //     String[] tickerList = this.tickerIndex.get();
    //     while (tickerList[i] != null) {
    //         while (true) {
    //             Order buyHead = this.buyOrders.get(i);
    //             Order sellHead = this.sellOrders.get(i);
    //             if (sellHead == null || buyHead == null || sellHead.price > buyHead.price) {
    //                 break;
    //             }

    //             if (buyHead.qty == sellHead.qty) {
    //                 this.buyOrders.compareAndSet(i, buyHead, buyHead.next.get());
    //                 this.sellOrders.compareAndSet(i, sellHead, sellHead.next.get());
    //             } else if (buyHead.qty > sellHead.qty) {
    //                 this.sellOrders.compareAndSet(i, sellHead, sellHead.next.get());
    //             } else {

    //             }

    //         }

    //         i++;
    //     }
    // }
}


public class main {
    
    public static void main(String[] args) {
        OrderBook ob = new OrderBook();

        // ob.addOrder("Sell", "AAA", 10, 20.0);
        // ob.addOrder("Sell", "AAA", 10, 5.0);
        // ob.addOrder("Sell", "AAA", 10, 40.0);

        // Order sOrder = ob.sellOrders.get(0);
        // while (sOrder != null) {
        //     System.out.println(sOrder.price);
        //     sOrder = sOrder.next.get();
        // }

        ob.addOrder("Buy", "AAA", 10, 20.0);
        ob.addOrder("Buy", "AAA", 10, 5.0);
        ob.addOrder("Buy", "AAA", 10, 40.0);

        Order bOrder = ob.buyOrders.get(0);
        while (bOrder != null) {
            System.out.println(bOrder.price);
            bOrder = bOrder.next.get();
        }
    }
}
