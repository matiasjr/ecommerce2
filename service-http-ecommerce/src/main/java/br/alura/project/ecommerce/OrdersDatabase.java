package br.alura.project.ecommerce;

import br.alura.project.ecommerce.database.LocalDatabase;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;

public class OrdersDatabase implements Closeable {
    private final LocalDatabase database;

    OrdersDatabase() throws SQLException {
        this.database = new LocalDatabase("orders_database");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key)");
    }

    public boolean saveNew(Order order) throws SQLException {
        if(wasProcessed(order)) {
            return false;
        }
        database.update("insert into Orders (uuid) values (?)", order.getOrderId());
        return true;
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return result.next();
    }

    @Override
    public void close() throws IOException {
        database.close();

    }
}
