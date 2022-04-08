package br.com.alura.ecommerce;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException {
        try {
            // we are not caring about any security issues, we are only
            // showing how to use http as a starting point

            String userEmail = req.getParameter("email");
            String orderId = req.getParameter("uuid"); // UUID.randomUUID().toString();
            BigDecimal amount = new BigDecimal(req.getParameter("amount"));

            Order order = new Order(orderId, amount, userEmail);

            try(OrdersDatabase database = new OrdersDatabase()) {
                if (database.saveNew(order)) {
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userEmail,
                            new CorrelationId(NewOrderServlet.class.getSimpleName()), order);

                    System.out.println("New order sent successfully.");

                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New order sent!");
                } else {
                    System.out.println("Old order received.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old order received.");
                }
            }
        } catch (ExecutionException | InterruptedException | SQLException | IOException e) {
            throw new ServletException(e);
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            orderDispatcher.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
