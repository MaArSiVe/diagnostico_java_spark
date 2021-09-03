package Utils;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.Common.OUTPUT_PATH;

public class PropertyReader {

    private String data ="";
    private String ruta ="";
    public PropertyReader() {
        Properties propiedades = new Properties();
        try {
            propiedades.load(new FileReader("src/test/resources/params.properties"));
            data = propiedades.getProperty("data");
            ruta = propiedades.getProperty("ruta");
        } catch (FileNotFoundException e) {
            System.out.print("No fue posible encontrar el archivo: config.properties");
        } catch (IOException e) {
            System.out.print("No fue posible leer el archivo: config.properties");
        }catch (Exception e) {
            System.out.print("Error");
        }
    }

    public String getData() {
        return data;
    }

    public String getRuta() {
        return ruta;
    }
}
