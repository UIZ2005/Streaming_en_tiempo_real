variable "resource_group_name" {
  description = "Nombre del grupo de recursos"
  type        = string
  default     = "rg-streaming-fraude"
}

variable "location" {
  description = "Región de Azure"
  type        = string
  default     = "chilecentral"
}

variable "vm_size" {
  description = "Tamaño de la VM. El stack completo necesita 8 GB de RAM como mínimo; con 16 GB hay holgura"
  type        = string
  default     = "Standard_B4as_v2"
}

variable "os_disk_gb" {
  description = "Tamaño del disco en GB (imágenes Docker + datos de Kafka e InfluxDB)"
  type        = number
  default     = 64
}

variable "ips_permitidas" {
  description = "IPs públicas con acceso a SSH y a las interfaces web, en formato CIDR. Ej: [\"203.0.113.10/32\"]"
  type        = list(string)
  # Sin default a propósito: obliga a decidir quién puede entrar
}
