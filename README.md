# Pipeline de streaming financiero: detección de fraude en tiempo real

Simulación de un sistema de monitoreo de transacciones bancarias. Los productores generan pagos, transferencias e inicios de sesión; Kafka los recibe, Flink los valida, calcula agregados por minuto y detecta fraude, Telegraf los lleva a InfluxDB y Grafana los muestra en vivo.

**Stack:** Kafka (KRaft) + Flink (PyFlink) + Telegraf + InfluxDB + Grafana, todo con Docker Compose.

## Arquitectura

```
Productores (Python) → Kafka → Flink → Kafka → Telegraf → InfluxDB → Grafana
```

El proyecto se puede ejecutar de dos formas:

| Opción | Dónde corre | Cuándo usarla |
|---|---|---|
| **A. Local** | Tu computador con Docker Desktop | Desarrollo y pruebas del día a día |
| **B. Azure** | Una VM creada con Terraform y configurada con Ansible | Demostración y pruebas de carga |

## Estructura del repositorio

```
├── docker-compose.yml        # Todo el stack
├── .env.example              # Plantilla de variables (el .env real no se sube)
├── kafka/                    # Script que crea los topics
├── productores/              # Productores en Python
├── flink/                    # Dockerfile y jobs de Flink
├── telegraf/                 # Configuración de Telegraf
├── grafana/                  # Datasource y dashboard (provisioning)
├── pruebas/                  # Scripts de latencia y mensajes malformados
├── docs/                     # Esquemas, capturas y reporte de rendimiento
├── terraform/                # Infraestructura en Azure (VM, red, firewall)
├── ansible/                  # Configuración de la VM (Docker y despliegue del stack)
├── Vagrantfile               # Nodo de control con Terraform, Ansible y Azure CLI
└── script.sh                 # Instala las herramientas en el nodo de control
```

## Variables de entorno

Antes de cualquier opción, crea tu `.env` a partir de la plantilla:

```bash
cp .env.example .env
```

| Variable | Qué es |
|---|---|
| `INFLUX_PASSWORD` | Contraseña del usuario `admin` de InfluxDB |
| `INFLUX_TOKEN` | Token de acceso a InfluxDB (usado por InfluxDB, Telegraf y Grafana) |
| `TASA_TARJETAS` | Eventos por segundo del productor de pagos |
| `TASA_TRANSFERENCIAS` | Eventos por segundo del productor de transferencias |
| `TASA_LOGINS` | Eventos por segundo del productor de logins |

Cambia los valores de ejemplo por contraseñas y un token propios. El `.env` contiene secretos: **no lo subas a GitHub** (ya está en `.gitignore`).

---

## Opción A. Ejecución local

### Requisitos
- Docker Desktop con al menos **8 GB de RAM** asignados (*Settings → Resources*)
- Git

### Pasos
```bash
git clone <URL-DEL-REPO>
cd <carpeta>
cp .env.example .env        # edita los valores
docker compose up -d --build
```

La primera vez tarda varios minutos porque descarga imágenes y construye la de Flink.

### Servicios y URLs (local)
| Servicio | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Flink | http://localhost:8081 |
| Grafana | http://localhost:3000 (admin / admin) |
| InfluxDB | http://localhost:8086 |

---

## Opción B. Aprovisionamiento en Azure (Terraform + Ansible)

Terraform crea la infraestructura en Azure (red, firewall, IP pública y una VM Ubuntu 22.04). Después ejecuta automáticamente un playbook de Ansible que instala Docker en la VM, copia el proyecto y levanta el stack con `docker compose up -d --build`.

Todo se lanza desde un **nodo de control**: una VM local (VirtualBox) creada con Vagrant, que ya trae Terraform, Ansible y Azure CLI.

```
Tu PC → Vagrant → [control-node: Terraform + Ansible] → Azure → [vm-streaming: Docker + stack]
```

### Requisitos
- [VirtualBox](https://www.virtualbox.org/) y [Vagrant](https://www.vagrantup.com/)
- Una suscripción de Azure con cuota de vCPU suficiente (la VM por defecto usa 4)
- Git
- El archivo `.env` creado (ver "Variables de entorno"). Terraform lo necesita para copiarlo a la VM

### Paso 1. Levantar el nodo de control
Desde la raíz del repositorio:

```bash
vagrant up
vagrant ssh
```

El primer `vagrant up` tarda varios minutos: instala Terraform, Ansible y Azure CLI, y genera la llave SSH `~/.ssh/azure_key` que se usará para entrar a la VM de Azure. La carpeta del repositorio queda disponible dentro del nodo en `/vagrant`.

### Paso 2. Iniciar sesión en Azure
Dentro del nodo de control (solo la primera vez):

```bash
az login --use-device-code
```

Abre el enlace que aparece, escribe el código y entra con tu cuenta. Si más adelante Terraform se queja de `subscription_id`, ejecuta:

```bash
export ARM_SUBSCRIPTION_ID=$(az account show --query id -o tsv)
```

### Paso 3. Configurar las variables de Terraform
```bash
cd /vagrant/terraform
cp terraform.tfvars.example terraform.tfvars
```

Edita `terraform.tfvars` y escribe **tu IP pública** (para conocerla: `curl -s ifconfig.me`):

```hcl
ips_permitidas = ["203.0.113.10/32"]
```

Solo las IPs de esa lista podrán entrar por SSH y ver las interfaces web (Kafka UI y Flink no tienen contraseña). Si tu IP cambia, por ejemplo al presentar desde otra red, actualiza este valor y vuelve a ejecutar `terraform apply`.

Variables opcionales (en `variables.tf`):

| Variable | Por defecto | Para qué |
|---|---|---|
| `location` | `chilecentral` | Región de Azure |
| `vm_size` | `Standard_B4as_v2` | Tamaño de la VM (4 vCPU, 16 GB) |
| `os_disk_gb` | `64` | Tamaño del disco |
| `resource_group_name` | `rg-streaming-fraude` | Nombre del grupo de recursos |

> **Pruebas de carga:** las series B son *burstable* y se limitan tras un uso sostenido de CPU, lo que falsea las mediciones. Para la prueba de rendimiento usa una serie no burstable, por ejemplo `vm_size = "Standard_D4as_v5"`.
>
> Para comprobar disponibilidad y cuota en tu región: `az vm list-skus --location chilecentral --size Standard_D4 --output table` y `az vm list-usage --location chilecentral -o table`.

### Paso 4. Crear la infraestructura y desplegar
```bash
terraform init      # descarga los proveedores (solo la primera vez)
terraform plan      # muestra qué se va a crear
terraform apply     # escribe "yes" para confirmar
```

`terraform apply` hace todo en orden: crea la red, el firewall y la VM, escribe el inventario de Ansible (`ansible/inventory.ini`) con la IP asignada y ejecuta el playbook. La primera vez tarda entre 10 y 20 minutos. La parte más lenta es la construcción de la imagen de Flink dentro de la VM.

Qué hace el playbook (`ansible/playbook.yml`):
1. Espera a que la VM responda por SSH.
2. Instala Docker y Docker Compose.
3. Copia `docker-compose.yml`, `.env` y las carpetas `kafka`, `productores`, `flink`, `telegraf` y `grafana` a `/opt/streaming`.
4. Ejecuta `docker compose up -d --build`.
5. Muestra el estado de los contenedores (`docker compose ps -a`).

Cuando los jobs de Flink ya existan, activa en el playbook `esperar_jobs_flink: true` para que Ansible verifique que los 2 jobs queden en `RUNNING`.

### Paso 5. Abrir los servicios
```bash
terraform output
```

Muestra la IP pública, el comando SSH y las URLs:

| Servicio | URL |
|---|---|
| Grafana | `http://<IP>:3000` (admin / admin) |
| Kafka UI | `http://<IP>:8080` |
| Flink | `http://<IP>:8081` |
| InfluxDB | `http://<IP>:8086` |

Para entrar a la VM y revisar los contenedores:

```bash
ssh -i ~/.ssh/azure_key azureuser@<IP>
cd /opt/streaming
docker compose ps
docker compose logs -f <servicio>
```

### Actualizar el despliegue después de cambios
Si modificas el compose, el `.env`, el playbook o cualquier archivo de `kafka/`, `productores/`, `flink/`, `telegraf/` o `grafana/`, basta con volver a ejecutar:

```bash
cd /vagrant/terraform
terraform apply
```

Terraform detecta el cambio y repite el despliegue con Ansible.

### Apagar y borrar (cuidado con los costos)
| Acción | Comando |
|---|---|
| Pausar la VM (deja de cobrar cómputo; la IP estática sigue costando poco) | `az vm deallocate -g rg-streaming-fraude -n vm-streaming` |
| Reanudar la VM | `az vm start -g rg-streaming-fraude -n vm-streaming` |
| **Borrar todo** (VM, red, IP, disco) | `terraform destroy` |
| Apagar el nodo de control | `vagrant halt` |

---

## Comandos útiles (Docker Compose)
Funcionan igual en local y dentro de la VM de Azure (en `/opt/streaming`).

| Qué | Comando |
|---|---|
| Estado de los servicios | `docker compose ps` |
| Logs de un servicio | `docker compose logs -f <servicio>` |
| Memoria y CPU por contenedor | `docker stats --no-stream` |
| Apagar | `docker compose down` |
| Apagar y borrar datos | `docker compose down -v` |

## Topics de Kafka

Los crea automáticamente el servicio `kafka-init` (3 particiones, retención de 24 h).

| Topic | Escribe | Lee | Contenido |
|---|---|---|---|
| `pagos-tarjeta` | Productor de tarjetas | Job 2 de Flink | Pagos crudos |
| `transferencias` | Productor de transferencias | Job 2 de Flink | Transferencias crudas |
| `logins` | Productor de logins | Job 2 de Flink | Inicios de sesión crudos |
| `eventos-validos` | Job 2 de Flink | Job 1 de Flink, Telegraf | Eventos que pasaron la validación |
| `agregados-minuto` | Job 1 de Flink | Telegraf | Conteo, promedio y máximo por minuto |
| `alertas-fraude` | Job 2 de Flink | Telegraf | Alertas de fraude |
| `dlq` | Job 2 de Flink | Telegraf | Mensajes mal formados con el motivo del error |

## Problemas comunes

| Síntoma | Causa probable | Solución |
|---|---|---|
| `terraform plan` falla con `subscription_id` | Terraform no encuentra la suscripción | `export ARM_SUBSCRIPTION_ID=$(az account show --query id -o tsv)` |
| `terraform plan` falla con `no file found` sobre `.env` | Falta el `.env` en la raíz | `cp .env.example .env` |
| `terraform plan` falla con `azure_key.pub` | Estás fuera del nodo de control o no se generó la llave | Ejecuta Terraform dentro de `vagrant ssh` y revisa que exista `~/.ssh/azure_key.pub` |
| Error de cuota o tamaño no disponible | La región no tiene ese tamaño o te falta cuota de vCPU | Cambia `vm_size` o `location` (ver comandos `az vm list-skus` / `list-usage`) |
| No abre las páginas web de la VM | Tu IP pública cambió | Actualiza `ips_permitidas` y ejecuta `terraform apply` |
| Ansible falla construyendo una imagen | Error en un Dockerfile de `productores/` o `flink/` | Revisa la salida del playbook o ejecuta `docker compose build` dentro de la VM |
| `kafka-init` falla con `\r: command not found` | El `.sh` se guardó con finales de línea de Windows | Cambia a `LF` en el editor (el `.gitattributes` del repo lo evita) |
| Los productores fallan con `NoBrokersAvailable` | Usan `localhost` dentro de un contenedor | Dentro de Docker es `kafka:9092`; desde el PC o la VM, `localhost:29092` |