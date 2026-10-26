import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.time.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;


public class WM_Central {

	private static int puertoEscucha;
	private static String ipKafka;
	private static int puertoKafka;
	private static ServerSocket skServidor;

	// Productor para enviar ordenes y respuestas
	private static KafkaProducer<String, String> productorKafka;
	// Planificador de tareas para controlar los tiempos
	private static final ScheduledExecutorService temporizadorRiego = Executors.newScheduledThreadPool(4);
	private static final ConcurrentHashMap<String, ScheduledFuture<?>> tareasTemporizadas = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Queue<String>> colasFO = new ConcurrentHashMap<>();

	private static final String TOPIC_PETICIONES_FO = "peticiones_riego";
	private static final String TOPIC_ORDENES_ENGINE = "ordenes-riego";
	private static final String TOPIC_RESPUESTAS_FO = "respuestas_riego";
	// CANAL PARA HABLAR SOBRE LOS DATOS DEL CAUDAL
	private static final String TOPIC_TELEMETRIA = "telemetria-estaciones";

	// variables para no acceder todo el tiempo a la base de datos
	private static final ConcurrentHashMap<String, Double> caudalActual = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Double> volumenActual = new ConcurrentHashMap<>();

	public static void main(String[] args) {
	
		// servidor sr = new servidor();
		if (args.length < 3) {
			System.out.println("Debe indicar el puerto de escucha de la central, IP de Kafka y puerto de Kafka");
			System.out.println("$./WM_Central puerto_central ip_kafka puerto_kafka");
			System.exit (1);
		}
		if(GestorCentral.getInstance().cargarDatos()){
			System.out.println("Datos cargados correctamente");
		}else{
			System.out.println("ERROR al cargar los datos");
			System.exit(1);
		}
		puertoEscucha = Integer.parseInt(args[0]);
		ipKafka = args[1];
		puertoKafka = Integer.parseInt(args[2]);

		iniciarProductorKafka();
		iniciarConsumidorKafkaFO();
		iniciarConsumidorTelemetria();

		// NOS ASEGURAMOS DE QUE CIERRE AL TERMINAR EL PROGRAMA
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			System.out.println("\n[SISTEMA] Apagando WM_Central de forma segura...");
			temporizadorRiego.shutdownNow();
			if (productorKafka != null) {
				productorKafka.flush();
				productorKafka.close();
			}
			if (skServidor != null && !skServidor.isClosed()) {
				try {
					skServidor.close();
				} catch (IOException ignored) {}
			}
			System.out.println("[SISTEMA] Central detenida.");
		}));
		Thread hiloSocket = new Thread(() -> {
			try{
				skServidor = new ServerSocket(puertoEscucha);
				for(;;) {
					try{
						Socket skCliente = skServidor.accept(); // Crea objeto
						System.out.println("Sirviendo cliente...");

						Thread t = new HiloServidor(skCliente);
						t.start();
					}catch(Exception e){
						if(skServidor.isClosed()){
							break;
						}
						System.out.println("Error: " + e.toString());

					}	
				}
			}catch(Exception e) {
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
		});

		hiloSocket.setDaemon(true);
		hiloSocket.start();
	}

	
	private static void iniciarProductorKafka() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		try{
			productorKafka = new KafkaProducer<>(props);
			System.out.println("Productor de Central a Kafka conectado");
		}catch(Exception e){
			System.out.println("ERROR al conectar el productor: " + e.getMessage());
		}
	}

	private static void iniciarConsumidorKafkaFO(){
		// SE EJECUTA EN PARALELO
		Thread hiloConsumidor = new Thread(() -> {
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-central-peticiones");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

			KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props);
			consumidor.subscribe(Collections.singletonList(TOPIC_PETICIONES_FO));

			try{
				while(true){
					// LEE DURANTE 1 SEGUNDO, SI LLEGA LO TRATA AL INSTANTE SI NO DEVUELVE NULL Y VUELVE AL BUCLE
					ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(1000));
					for(ConsumerRecord<String, String> record : records){
						String mensaje = record.value();
						if(mensaje == null || mensaje.isEmpty()){
							continue;
						}
						String[] ordenes = mensaje.split(";");
						if(ordenes.length > 1){
							encolarDoc(ordenes, record.key());
						}else{
							procesarPeticionFO(ordenes[0]);
						}
					}
				}
			}catch(Exception e){
				System.out.println("ERROR en el hilo consumidor: " + e.getMessage());
			}finally{
				consumidor.close();
			}
		});
		// PARA QUE AL CERRAR EL PROGRAMA EL PROCESO MUERA
		hiloConsumidor.setDaemon(true);
		hiloConsumidor.start();
	}

	private static void encolarDoc(String[] ordenes, String key){
		Queue<String> cola = new LinkedList<>();
		String idFO = key;

		for(String p : ordenes){
			String peticion = p.trim();
			if(!peticion.isEmpty()){
				cola.add(peticion);
				if(idFO == null && peticion.split("#").length >= 3){
					idFO = peticion.split("#")[2].trim();
				}
			}
		}

		if(idFO != null && !cola.isEmpty()){
			colasFO.put(idFO, cola);
			siguienteTareaDoc(idFO);
		}
	}

	private static void siguienteTareaDoc(String idFO){
		Queue<String> cola = colasFO.get(idFO);
		if(cola != null && !cola.isEmpty()){
			String peticion = cola.poll();
			procesarPeticionFO(peticion);
		}else{
			colasFO.remove(idFO);
		}
	}

	private static void espera(String idFO){
		if(idFO != null && colasFO.containsKey(idFO) && !colasFO.get(idFO).isEmpty()){
			temporizadorRiego.schedule(() ->{
				siguienteTareaDoc(idFO);
			}, 4, TimeUnit.SECONDS);
		}
	}


	private static void iniciarConsumidorTelemetria(){
		Thread hiloTelemetria = new Thread(() ->{
			Properties props = new Properties();
			props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ipKafka + ":" + puertoKafka);
			props.put(ConsumerConfig.GROUP_ID_CONFIG, "grupo-central-telemetria");
			props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
			props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

			KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(props);
			consumidor.subscribe(Collections.singletonList(TOPIC_TELEMETRIA));

			try{
				while(true){
					ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(1000));
					for(ConsumerRecord<String, String> record : records){
						// TELEMETRIA#idWS#caudal#volumenAcumulado#timestamp
						String[] partes = record.value().split("#");
						if(partes.length >= 4 && partes[0].equals("TELEMETRIA")){
							String idWS = partes[1].trim();
							double caudal = Double.parseDouble(partes[2].trim());
							double volumen = Double.parseDouble(partes[3].trim());
							caudalActual.put(idWS, caudal);
							volumenActual.put(idWS, volumen);
						}
					}
				}
			}catch(Exception e){
				System.out.println("ERROR en el hilo telemetria: " + e.getMessage());
			}finally{
				consumidor.close();
			}
		});

		hiloTelemetria.setDaemon(true);
		hiloTelemetria.start();
	}


	private static void procesarPeticionFO(String mensaje){
		String[] partes = mensaje.split("#");
		if(partes.length < 2){
			return;
		}
		String accion = partes[0].trim();
		String idWS  = partes[1].trim();

		if(accion.equals("START_RIEGO") && partes.length == 4){
			String idFO = partes[2].trim();
			try {
				int duracion = Integer.parseInt(partes[3].trim());

				String valido = GestorCentral.getInstance().procesarPeticion(mensaje);
				if(valido.equals("<ACK>")){
					volumenActual.put(idWS, 0.);
					enviarEngine("INICIAR_RIEGO#" + idWS, idWS);
					enviarFO(idWS, idFO, "ACK#" + idWS);

					if(duracion > 0){
						tiempoRiego(idWS, idFO, duracion);
					}else{

					}
				}else{
					System.out.println("Riego rechazado para " + idWS);
					enviarFO(idWS, idFO, "NACK#" + idWS);
					espera(idFO);
				}
			}catch(NumberFormatException e){
				System.out.println("ERROR en el formato de la duracion");
				espera(idFO);
			}
		}else if(accion.equals("STOP_RIEGO") && partes.length >= 2){
			String idFO;
			if(partes.length >= 3){
				idFO = partes[2].trim();
			}else{
				idFO = "DESCONOCIDO";
			}

			cancelarTiempo(idWS);
			GestorCentral.getInstance().procesarPeticion(mensaje);
			enviarEngine("PARAR_RIEGO#" + idWS, idWS);
			double litrosTotales = volumenActual.getOrDefault(idWS, 0.);
			enviarFO(idWS, idFO, "FIN_RIEGO#" + idWS + "#" + litrosTotales);
			espera(idFO);
		}
	}

	private static void tiempoRiego(String idWS, String idFO, int duracion){
		cancelarTiempo(idWS);

		ScheduledFuture<?> tarea = temporizadorRiego.schedule(() -> {
			// lo que se va a ejecutar cuando la cuenta atrás llegue a cero
			GestorCentral.getInstance().finalizarRiego(idWS);
			enviarEngine("PARAR_RIEGO#" + idWS, idWS);
			double litrosTotales = volumenActual.getOrDefault(idWS, 0.);
			enviarFO(idWS, idFO, "FIN_RIEGO#" + idWS + "#" + litrosTotales);

			tareasTemporizadas.remove(idWS);
			espera(idFO);
		}, duracion, TimeUnit.MINUTES);

		tareasTemporizadas.put(idWS, tarea);
	}

	private static void cancelarTiempo(String idWS){
		ScheduledFuture<?> tarea = tareasTemporizadas.remove(idWS);
		if(tarea != null && !tarea.isDone()){
			tarea.cancel(false);
		}
	}


	public static void enviarEngine(String com, String idWS){
		if(productorKafka == null){
			return;
		}
		ProducerRecord<String, String> ticket = new ProducerRecord<>(TOPIC_ORDENES_ENGINE, idWS, com);
		productorKafka.send(ticket, (metadata, exception) -> {
			if(exception == null){
				System.out.println("Enviado " + com + " a topic " + TOPIC_ORDENES_ENGINE);
			}else{
				System.out.println("ERROR al enviar orden al Engine");
			}
		});
	}

	public static void enviarFO(String idWS, String idFO, String res){
		if(productorKafka == null){
			return;
		}
		ProducerRecord<String, String> ticket = new ProducerRecord<>(TOPIC_RESPUESTAS_FO, idFO, res);
		productorKafka.send(ticket, (metadata, exception) -> {
			if(exception == null){
				System.out.println("Respuesta enviada");
			}
		});
	}

	public static void cortarAguaFuga(String idWS){
		System.out.println("¡ALERTA! Fuga detectada en " + idWS);
		String idFO = GestorCentral.getInstance().obtenerOperario(idWS);
		cancelarTiempo(idWS);
		enviarEngine("PARAR_RIEGO#" + idWS, idWS);
		if(idFO != null){
			enviarFO(idWS, idFO, "FUGA#" + idWS);
			espera(idFO);
		}
	}
}



