public class ProcesarFicheroPersonas {

    private static final boolean developmentMode = true;

    public static void main(String[] args){
        // PASO 1: Abrir la conexión con el cluster de Spark
        var builder = SparkSession.builder()
                .appNamcde("Procesar fichero Personas");
        if(developmentMode)
            builder.master("local[2]");
        SparkSession conexion = builder.getOrCreate();

        // PASO 2: Hacernos con los datos, en este caso no usaremos los RDDs, sino los Datasets
        Dataset<Row> datosPersonas = conexion.read()
                .json("src/main/resources/personas.json");
        Dataset<Row> datosCps = conexion.read()
                .option("header", "true")
                .option("sep", ",")
                .csv("src/main/resources/cps.csv");

        // PASO 3: Procesar los datos
        var miFuncionValidacion = udf( (String dni) -> Persona.validarDNI(dni), DataTypes.BooleanType);
        conexion.udf().register("esUnDniValido", miFuncionValidacion);
                                /// ^ Es el nombre que usaré en SQL

        // Registrar la tabla para poder usarla con sintaxis SQL
        datosPersonas.createOrReplaceTempView("personas");

        var personasConDNIValido = conexion.sql("SELECT * FROM personas WHERE esUnDniValido(dni)");
        var personasConDNIInvalido = conexion.sql("SELECT * FROM personas WHERE not esUnDniValido(dni)");

        var mayoresDeEdadConDNIValido = personasConDNIValido.filter(col("edad").geq(18));
        var menoresDeEdadConDNIValido = personasConDNIValido.filter(col("edad").lt(18));

        var datosFinales = mayoresDeEdadConDNIValido.join(datosCps, "cp");
        var cpNoDetectados = mayoresDeEdadConDNIValido.select("nombre","apellidos","edad","dni","cp","email")
                                  .except(datosFinales.select("nombre","apellidos","edad","dni","cp","email"));

        // PASO 4: Mostrar los resultados
        guardarDatos(personasConDNIInvalido, "src/main/resources/personasConDNIInvalido");
        guardarDatos(menoresDeEdadConDNIValido, "src/main/resources/menoresDeEdadConDNIValido");
        guardarDatos(datosFinales, "src/main/resources/datosFinales");
        guardarDatos(cpNoDetectados, "src/main/resources/cpNoDetectados");

        // Sleep de 1 hora para que me de tiempo a ver la interfaz gráfica en desarrollo
        if(developmentMode)
            try {
                Thread.sleep(60*60*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        // PASO 5: cerrar la conexión
        conexion.close();
    }
    private static void guardarDatos(Dataset<Row> datos, String ruta) {
        System.out.println("Guardando datos en " + ruta);
        if(!developmentMode) {
            datos.write()
                    .mode("overwrite")
                    .parquet(ruta);
        }else{
            datos.show();
        }
    }
}
