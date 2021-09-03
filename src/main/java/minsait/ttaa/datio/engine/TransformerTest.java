package minsait.ttaa.datio.engine;

import Utils.PropertyReader;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;

public class TransformerTest extends Writer {
    private SparkSession spark;
    private Dataset<Row> df;

    public TransformerTest(@NotNull SparkSession spark) {
        this.spark = spark;
        df = readInput();

        df.printSchema();

        df = cleanData(df);
    }

    public int ejercicioTest(int opcion)
    {
        Long contador = null;
        df = ageCheckFunction(df);
        df = rankByNationalityPosFunction(df);
        df = potentialVsOverallFunction(df);
        contador = filterEjercicio5(df, opcion);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        return contador.intValue();
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                long_name.column(),
                age.column(),
                height_cm.column(),
                weight_kg.column(),
                nationality.column(),
                club_name.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                age_range.column(),
                rank_by_nationality_position.column(),
                potential_vs_overall.column()
                //  cat_height_by_position.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        PropertyReader propertyReader = new PropertyReader();
        String data;
        data = propertyReader.getData();
        if (data.equals(""))
            data = INPUT_PATH;

        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(data);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @return un Dataset con un filtro de transformacion aplicado
     * columna age_range agregada que contiene el valor:
     * A si el jugador es menor de 23 años
     * B si el jugador es menor de 27 años
     * C si el jugador es menor de 32 años
     * D si el jugador tiene 32 años o más
     */
    private Dataset<Row> ageCheckFunction(Dataset<Row> df) {
        Column rule = when(age.column().$less(23), VALUE_A)
                .when(age.column().$less(27), VALUE_B)
                .when(age.column().$less(32), VALUE_C)
                .otherwise(VALUE_D);

        df = df.withColumn(age_range.getName(), rule);

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
 /**   private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), VALUE_A)
                .when(rank.$less(50), VALUE_B)
                .otherwise(VALUE_C);

        df = df.withColumn(catHeightByPosition.getName(), row_number());

        return df;
    }*/

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @return un Dataset con un filtro de transformacion aplicado
     * columna rank_by_nationality_position agregada que contiene la siguiente regla:
     * Para cada país (nationality) y posición(team_position)
     * debemos ordenar a los jugadores por la columna overall de forma descendente
     * y colocarles un número generado por la función row_number
     */
    private Dataset<Row> rankByNationalityPosFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall .column().desc());

        df = df.withColumn(rank_by_nationality_position.getName(), row_number().over(w));

        return df;
    }

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @return un Dataset con un filtro de transformacion aplicado
     * columna potential_vs_overall agregada que contiene la siguiente regla:
     * potential_vs_overall = potential/overall
     */
    private Dataset<Row> potentialVsOverallFunction(Dataset<Row> df) {
        Column rule = potential.column().divide(overall.column());

        df = df.withColumn(potential_vs_overall.getName(), rule);
        return df;
    }

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @param opcion
     * @return un Dataset con un filtro de transformacion aplicado
     * Metodo para probar distintos tipos de filtro
     */
    private Long filterEjercicio5(Dataset<Row> df, int opcion) {

        switch (opcion)
        {
            case 1:
                return filterEj51ByNationalityPos(df);
            case 2:
                return filterEj52ByAgeRangePvO(df);
            default:
                return new Long(0);
        }

       // df = filterEj52ByAgeRangePvO(df);
       // df = filterEj53ByAgeRangePvO(df);
       // df = filterEj54ByAgeRangePvO(df);

    }

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @return un Dataset con un filtro de transformacion aplicado     * Metodo para probar distintos tipos de filtro:
     * Si rank_by_nationality_position es menor a 3
     */
    private Long filterEj51ByNationalityPos(Dataset<Row> df) {
        df = df.filter(rank_by_nationality_position.column().$less(3));

        return df.where(rank_by_nationality_position.column().$greater(3)).count();
    }

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @return un Dataset con un filtro de transformacion aplicado
     * Metodo para probar distintos tipos de filtro:
     * Si age_range es B o C y potential_vs_overall es superior a 1.15
     */
    private Long filterEj52ByAgeRangePvO(Dataset<Row> df) {
        df = df.filter(
                (age_range.column().equalTo(VALUE_B).or(age_range.column().equalTo(VALUE_C))
                        .and(potential_vs_overall.column().$greater(1.15)))
        );

        return df.where(potential_vs_overall.column().$less(1.15)).count();
    }

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @return un Dataset con un filtro de transformacion aplicado
     * Metodo para probar distintos tipos de filtro:
     * Si age_range es A y potential_vs_overall es superior a 1.25
     */
    private Dataset<Row> filterEj53ByAgeRangePvO(Dataset<Row> df) {
        df = df.filter(age_range.column().equalTo(VALUE_A)
                .and(potential_vs_overall.column().$greater(1.25))
        );

        return df;
    }

    /**
     * Creado por Manuel Silva 02/09/2021
     * @param df
     * @return un Dataset con un filtro de transformacion aplicado
     * Metodo para probar distintos tipos de filtro:
     * Si age_range es D y rank_by_nationality_position es menor a 5
     */
    private Dataset<Row> filterEj54ByAgeRangePvO(Dataset<Row> df) {
        df = df.filter(age_range.column().equalTo(VALUE_D)
                .and(potential_vs_overall.column().$less(5))
        );
        return df;
    }
}
