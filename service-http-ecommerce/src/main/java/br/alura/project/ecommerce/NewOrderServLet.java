package br.alura.project.ecommerce;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServLet extends HttpServlet {

    private final kafkaDispatcher<Order> orderDispatcher = new kafkaDispatcher<>();
    private final kafkaDispatcher<String> emailDispatcher = new kafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
        emailDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {
            // we are not caring about security issues, we are only
            // showing how to use http as a start point
            var email = req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));

            var orderId = UUID.randomUUID().toString();

            var order = new Order(orderId, amount, email);
            orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                    new CorrelationId(NewOrderServLet.class.getSimpleName()),
                    order);

            var emailCode = "Thank you for your Order!!!. We are processing your order.";
            emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email,
                    new CorrelationId(NewOrderServLet.class.getSimpleName()),
                    emailCode);

            System.out.println("New Order sent successfully.");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("New Order sent");

        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }

}

