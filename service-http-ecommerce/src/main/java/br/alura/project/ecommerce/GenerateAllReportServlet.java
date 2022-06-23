package br.alura.project.ecommerce;

import br.alura.project.ecommerce.dispatcher.kafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GenerateAllReportServlet extends HttpServlet {

    private final kafkaDispatcher<String> batchDispatcher = new kafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        batchDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try {

            batchDispatcher.send("ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS", "ECOMMERCE_USER_GENERATE_READING_REPORT",
                    new CorrelationId(GenerateAllReportServlet.class.getSimpleName()),
                    "ECOMMERCE_USER_GENERATE_READING_REPORT" );

            System.out.println("Sent generate report to all users");
            resp.setStatus(HttpServletResponse.SC_OK);
            resp.getWriter().println("Reports requests generated");
        } catch (ExecutionException | InterruptedException e) {
            throw new ServletException(e);
        }
    }

}

