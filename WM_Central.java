import java.io.*;
import java.net.*;

public class WM_Central {

	private static int puertoEscucha;
	private static String ipKafka;
	private static int puertoKafka;
	private static ServerSocket skServidor;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			// servidor sr = new servidor();
			if (args.length < 3) {
				System.out.println("Debe indicar el puerto de escucha de la central, IP de Kafka y puerto de Kafka");
				System.out.println("$./WM_Central puerto_central ip_kafka puerto_kafka");
				System.exit (1);
			}
			if(GestorCentral.getInstance().cargarDatos()){
				System.out.println("Datos cargados correctamente");
			}else{
				System.out.println("Error al cargar los datos");
				System.exit(1);
			}
			puertoEscucha = Integer.parseInt(args[0]);
			ipKafka = args[1];
			puertoKafka = Integer.parseInt(args[2]);
			skServidor = new ServerSocket(puertoEscucha);
			System.out.println("Escucho el puerto " + puertoEscucha + " para recibir peticiones de los clientes");

			// ARRANCAR KAFKA E INTERFACES 
	

			

			
			
			for(;;) {
				try{
					Socket skCliente = skServidor.accept(); // Crea objeto
					System.out.println("Sirviendo cliente...");

					Thread t = new HiloServidor(skCliente);
					t.start();
				}catch(Exception e){
					System.out.println("Error: " + e.toString());
				}	
			}
		}
		catch(Exception e) {
			System.out.println("Error: " + e.toString());
		}finally {
			if(skServidor != null) {
				try {
					skServidor.close();
				} catch (IOException e) {
					System.out.println("Error al cerrar el socket del servidor: " + e.getMessage());
				}
			}
		}
		
	}

}

