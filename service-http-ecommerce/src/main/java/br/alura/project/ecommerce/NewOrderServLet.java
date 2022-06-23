package br.alura.project.ecommerce;

import br.alura.project.ecommerce.dispatcher.kafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class NewOrderServLet extends HttpServlet {

    private final kafkaDispatcher<Order> orderDispatcher = new kafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            // we are not caring about security issues, we are only
            // showing how to use http as a start point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = req.getParameter("uuid");
            var order = new Order(orderId, amount, email);

            try (var database = new OrdersDatabase()) {

                if (database.saveNew(order)) {
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                            new CorrelationId(NewOrderServLet.class.getSimpleName()),
                            order);

                    System.out.println("New Order sent successfully.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("New Order sent");
                } else {
                    System.out.println("Old Order received.");
                    resp.setStatus(HttpServletResponse.SC_OK);
                    resp.getWriter().println("Old Order received");
                }
            }

        } catch (ExecutionException | InterruptedException | SQLException e) {
            throw new ServletException(e);
        }
    }

}

