output "ip_publica" {
  value = azurerm_public_ip.public_ip.ip_address
}

output "ssh" {
  value = "ssh -i ~/.ssh/azure_key azureuser@${azurerm_public_ip.public_ip.ip_address}"
}

output "grafana_url" {
  value = "http://${azurerm_public_ip.public_ip.ip_address}:3000"
}

output "kafka_ui_url" {
  value = "http://${azurerm_public_ip.public_ip.ip_address}:8080"
}

output "flink_url" {
  value = "http://${azurerm_public_ip.public_ip.ip_address}:8081"
}

output "influxdb_url" {
  value = "http://${azurerm_public_ip.public_ip.ip_address}:8086"
}
