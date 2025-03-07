package src;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
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

class MatchResult {
    String ticker;
    int qty;
    double sellPrice;
    double buyPrice;

    MatchResult(String ticker, int qty, double buyPrice, double sellPrice) {
        this.ticker = ticker;
        this.qty = qty;
        this.buyPrice = buyPrice;
        this.sellPrice = sellPrice;
    }
}

class TickerOrder {
    final Order buyOrders;
    final Order sellOrders;

    TickerOrder(Order buyOrders, Order sellOrders) {
        this.buyOrders = buyOrders;
        this.sellOrders = sellOrders;
    }
}

class OrderBook {
    final AtomicReference<String[]> tickerIndex;
    final AtomicReferenceArray<TickerOrder> orders;

    OrderBook() {
        this.tickerIndex = new AtomicReference<>(new String[1024]);
        this.orders = new AtomicReferenceArray<>(1024);
    }

    int getTickerIndex(String ticker) {
        while (true) {
            String[] original = this.tickerIndex.get();
            int i = 0;
            while (i < original.length && original[i] != null) {
                if (original[i].equals(ticker)) {
                    return i;
                }
                i++;
            }

            String[] indices = Arrays.copyOf(original, original.length);
            if (i >= 1024) {
                System.out.println(Arrays.toString(original));
            }
            indices[i] = ticker;
            
            if (this.tickerIndex.compareAndSet(original, indices)) {
                return i;
            }
        }
    }

    public void addOrder(String orderType, String ticker, int qty, double price) {
        int index = this.getTickerIndex(ticker);

        TickerOrder to = this.orders.get(index);
        if (to == null) {
            TickerOrder newTickerOrder = new TickerOrder(null, null);
            this.orders.compareAndSet(index, null, newTickerOrder);
        }

        if (orderType.equals("Sell")) {
            this.addSellOrder(index, orderType, ticker, qty, price);
        } else if (orderType.equals("Buy")) {
            this.addBuyOrder(index, orderType, ticker, qty, price);
        }

        System.out.printf("[%s]\t[%s]\t%s \t@%s\n", orderType, ticker, qty, price);
    }

    void addSellOrder(int index, String orderType, String ticker, int qty, double price) {
        
        while (true) {

            TickerOrder tickerOrder = this.orders.get(index);
            Order newOrder = new Order(orderType, ticker, qty, price);
            Order sellHead = tickerOrder.sellOrders;

            if (sellHead == null) {
                TickerOrder newTickerOrder = new TickerOrder(tickerOrder.buyOrders, newOrder);
                if (this.orders.compareAndSet(index, tickerOrder, newTickerOrder)) {
                    return;
                }
            } else if (newOrder.price <= sellHead.price){
                newOrder.next.set(sellHead);
                TickerOrder newTickerOrder = new TickerOrder(tickerOrder.buyOrders, newOrder);
                if (this.orders.compareAndSet(index, tickerOrder, newTickerOrder)) {
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

            TickerOrder tickerOrder = this.orders.get(index);
            Order buyHead = tickerOrder.buyOrders;
            Order newOrder = new Order(orderType, ticker, qty, price);

            if (buyHead == null) {
                TickerOrder newTickerOrder = new TickerOrder(newOrder, tickerOrder.sellOrders);
                if (this.orders.compareAndSet(index, tickerOrder, newTickerOrder)) {
                    return;
                }
            } else if (newOrder.price >= buyHead.price){
                newOrder.next.set(buyHead);
                TickerOrder newTickerOrder = new TickerOrder(newOrder, tickerOrder.sellOrders);
                if (this.orders.compareAndSet(index, tickerOrder, newTickerOrder)) {
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

    public List<MatchResult> matchOrders() {
        int i = 0;
        String[] tickerList = this.tickerIndex.get();
        List<MatchResult> result = new ArrayList<>();

        while (tickerList[i] != null) {
            while (true) {
                TickerOrder to = this.orders.get(i);
                Order buyHead = to.buyOrders;
                Order sellHead = to.sellOrders;
                if (sellHead == null || buyHead == null || sellHead.price > buyHead.price) {
                    break;
                }

                if (buyHead.qty == sellHead.qty) {
                    TickerOrder newTickerOrder = new TickerOrder(buyHead.next.get(), sellHead.next.get());
                    if (this.orders.compareAndSet(i, to, newTickerOrder)) {
                        result.add(new MatchResult(buyHead.ticker, buyHead.qty, buyHead.price, sellHead.price));
                        System.out.printf("Matched order QTY:%s %s B:%s S:%s\n", buyHead.ticker, buyHead.qty, buyHead.price, sellHead.price);
                    }
                } else if (buyHead.qty > sellHead.qty) {
                    int remainingQty = buyHead.qty - sellHead.qty;
                    Order newBuyHead = new Order("Buy", tickerList[i], remainingQty, buyHead.price);
                    newBuyHead.next.set(buyHead.next.get());
                    TickerOrder newTickerOrder = new TickerOrder(newBuyHead, sellHead.next.get());

                    if (this.orders.compareAndSet(i, to, newTickerOrder)) {
                        result.add(new MatchResult(buyHead.ticker, sellHead.qty, buyHead.price, sellHead.price));
                        System.out.printf("Matched order QTY:%s %s B:%s S:%s\n", buyHead.ticker, sellHead.qty, buyHead.price, sellHead.price);
                    }
                } else {
                    int remainingQty = sellHead.qty - buyHead.qty;
                    Order newSellHead = new Order("Sell", tickerList[i], remainingQty, sellHead.price);
                    newSellHead.next.set(sellHead.next.get());
                    TickerOrder newTickerOrder = new TickerOrder(buyHead.next.get(), newSellHead);

                    if (this.orders.compareAndSet(i, to, newTickerOrder)) {
                        result.add(new MatchResult(buyHead.ticker, buyHead.qty, buyHead.price, sellHead.price));
                        System.out.printf("Matched order QTY:%s %s B:%s S:%s\n", buyHead.ticker, buyHead.qty, buyHead.price, sellHead.price);
                    }
                }

            }

            i++;
        }
        return result;
    }
}

// Wrapper to automatically execute addOrder function
class OrderAdder implements Runnable {
    private final OrderBook ob;
    private final Random random;

    OrderAdder(OrderBook ob) {
        this.ob = ob;
        this.random = new Random();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            List<String> bs = List.of("Buy", "Sell");
            String orderType = bs.get(this.random.nextInt(bs.size()));

            char l1 = (char) ('A' + random.nextInt(26));
            char l2 = (char) ('A' + random.nextInt(26));
            String ticker = "" + l1 + l2;

            int qty = this.random.nextInt(100) + 1;
            double price = this.random.nextDouble() * 100;

            this.ob.addOrder(orderType, ticker, qty, price);

            try {
                Thread.sleep(Constants.AUTO_ADD_DELAY);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}


public class main {
    
    public static void main(String[] args) {
        OrderBook ob = new OrderBook();
        TickerOrder to;

        Thread adderThread1 = new Thread(new OrderAdder(ob));
        Thread adderThread2 = new Thread(new OrderAdder(ob));
        adderThread1.start();
        adderThread2.start();

        for (int i = 0; i < Constants.MATCH_COUNT; i++) {
            try {
                Thread.sleep(Constants.DELAY_BETWEEN_MATCH);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    
            ob.matchOrders();
        }

        adderThread1.interrupt();
        adderThread2.interrupt();
    }
}
