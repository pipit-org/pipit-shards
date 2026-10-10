# Pipit Shards

Manejador de Shards de Pipit. Una shard es una conexión a lectura/escritura a una instancia de la base de datos.

En un sistema distribuido clásico cada shard podría ser una base de datos distinta, pero en este caso se reutiliza la misma base de datos sqlite para simplificar el código.

## Introducción

Una `Shard` cumple con la interfaz de un `http.Handler` por lo que pueden manejar peticiones HTTP provenientes de la red y así responder de manera independiente a los clientes.

Por otro lado, el `ShardManager` se encarga de crear y gestionar las conexiones abiertas. La implementación subyacente de Go con `database/sql` ya implementa un mecánismo de pooling para cada conexión de la base de datos, así se reutilizan de manera eficiente las conexiones abiertas.

## Relevante

El presente software se distribuye sin garantías de ningún tipo. La intención original de este programa es servir como proyecto de ejemplo para la asignatura "Desarrollo de Aplicaciones para Ambientes Distribuidos" en el IES6017, Salta, Argentina.
