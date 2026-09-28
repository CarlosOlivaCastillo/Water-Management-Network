import java.io.*;
import java.net.*;

public class WM_Central {

	private int puertoEscucha;
	private String ipKafka;
	private int puertoKafka;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int resultado=0;
		String Cadena="";
		String puerto="";
		try
		{
			// servidor sr = new servidor();
			if (args.length < 3) {
				System.out.println("Debe indicar el puerto de escucha de la central, IP de Kafka y puerto de Kafka");
				System.out.println("$./WM_Central puerto_central ip_kafka puerto_kafka");
				System.exit (1);
			}
			puertoEscucha = Integer.parseInt(args[0]);
			ipKafka = args[1];
			puertoKafka = Integer.parseInt(args[2]);
			ServerSocket skServidor = new ServerSocket(puertoEscucha);
		    System.out.println("Escucho el puerto " + puertoEscucha + " para recibir peticiones de los clientes");
			
	
			
			for(;;)
			{
				
				Socket skCliente = skServidor.accept(); // Crea objeto
		        System.out.println("Sirviendo cliente...");
				while (resultado != -1)
				{
										
				}
				
				skCliente.close();
				System.exit(0);				
			}
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.toString());
		}
	}

}
