package com.unitarytest;

import minsait.ttaa.datio.engine.TransformerTest;
import org.apache.spark.sql.SparkSession;
import org.junit.Assert;
import org.junit.Test;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

/**
 * Creado por Manuel Silva 02/09/2021
 */
public class UnitaryTestEjercicio {
    int contador;
    SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();
    TransformerTest engine = new TransformerTest(spark);
    @Test
    public void testEjercicio5Filtro1() {
        contador = engine.ejercicioTest(1);
        Assert.assertEquals(0,contador);
    }

    @Test
    public void testEjercicio5Filtro2() {
    contador = engine.ejercicioTest(2);
        Assert.assertEquals(0,contador);
    }
}
