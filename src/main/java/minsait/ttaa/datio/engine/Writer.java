package minsait.ttaa.datio.engine;

import Utils.PropertyReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.nationality;
import static org.apache.spark.sql.SaveMode.Overwrite;

abstract class Writer {
    static void write(Dataset<Row> df) {
        PropertyReader propertyReader = new PropertyReader();
        String ruta;
        ruta = propertyReader.getRuta();
        if (ruta.equals(""))
            ruta = INPUT_PATH;

        df
                .coalesce(1)
                .write()
                .partitionBy(nationality.getName())
                .mode(Overwrite)
                .parquet(ruta);
    }

}
