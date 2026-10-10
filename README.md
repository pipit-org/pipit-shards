# Pipit Shards

Gestor de fragmentos (*shards*) para el sistema de mensajería **Pipit**.

## ¿Qué es?

Una *shard* representa una conexión de lectura y escritura a una instancia de base de datos. 
* En un sistema distribuido tradicional, cada *shard* correspondería a una base de datos diferente; sin embargo, en este proyecto se reutiliza una única base de datos SQLite para simplificar la implementación.

## Componentes principales

* **Shard:** Implementa la interfaz `http.Handler`, lo que le permite procesar peticiones HTTP de la red y responder de forma independiente a los clientes.
* **ShardManager:** Se encarga de crear y administrar las conexiones abiertas. Utiliza el mecanismo nativo de *pooling* de `database/sql` en Go para optimizar la reutilización de conexiones de manera eficiente.

## Contexto

Software desarrollado como proyecto de ejemplo para la asignatura **Desarrollo de Aplicaciones para Ambientes Distribuidos** del **IES6017** (Salta, Argentina).

> Este software se distribuye sin garantías de ningún tipo.
