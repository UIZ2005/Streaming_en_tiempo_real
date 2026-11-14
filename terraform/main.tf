resource "azurerm_resource_group" "rg" {
  name     = var.resource_group_name
  location = var.location
}

resource "azurerm_virtual_network" "vnet" {
  name                = "vnet-streaming"
  address_space       = ["10.0.0.0/16"]
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
}

resource "azurerm_subnet" "subnet" {
  name                 = "subnet-streaming"
  resource_group_name  = azurerm_resource_group.rg.name
  virtual_network_name = azurerm_virtual_network.vnet.name
  address_prefixes     = ["10.0.1.0/24"]
}

# Firewall: solo las IPs de var.ips_permitidas pueden entrar
resource "azurerm_network_security_group" "nsg" {
  name                = "nsg-streaming"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name

  security_rule {
    name                       = "ssh"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     =  ["22", "80", "8080", "3000","8081","8086"]
    source_address_prefixes    = "*"
    destination_address_prefix = "*"
  }

  # 3000 Grafana | 8080 Kafka UI | 8081 Flink | 8086 InfluxDB (API REST)
  security_rule {
    name                       = "interfaces-web"
    priority                   = 110
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["3000", "8080", "8081", "8086"]
    source_address_prefixes    = var.ips_permitidas
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "nsg_assoc" {
  subnet_id                 = azurerm_subnet.subnet.id
  network_security_group_id = azurerm_network_security_group.nsg.id
}

# ---------- VM ----------

resource "azurerm_public_ip" "public_ip" {
  name                = "streaming-ip"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  allocation_method   = "Static"
  sku                 = "Standard"
}

resource "azurerm_network_interface" "nic" {
  name                = "streaming-nic"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name

  ip_configuration {
    name                          = "internal"
    subnet_id                     = azurerm_subnet.subnet.id
    private_ip_address_allocation = "Static"
    private_ip_address            = "10.0.1.10"
    public_ip_address_id          = azurerm_public_ip.public_ip.id
  }
}

resource "azurerm_linux_virtual_machine" "vm_streaming" {
  name                  = "vm-streaming"
  resource_group_name   = azurerm_resource_group.rg.name
  location              = var.location
  size                  = var.vm_size
  admin_username        = "azureuser"
  network_interface_ids = [azurerm_network_interface.nic.id]

  admin_ssh_key {
    username   = "azureuser"
    public_key = file(pathexpand("~/.ssh/azure_key.pub"))
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS" # SSD: Kafka e InfluxDB escriben mucho
    disk_size_gb         = var.os_disk_gb
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }
}

# ---------- Ansible ----------

# Terraform escribe el inventario con la IP pública que Azure asignó
resource "local_file" "inventario" {
  filename = "${path.module}/../ansible/inventory.ini"
  content  = <<-EOT
    [streaming]
    vm-streaming ansible_host=${azurerm_public_ip.public_ip.ip_address}

    [all:vars]
    ansible_user=azureuser
    ansible_ssh_private_key_file=~/.ssh/azure_key
    ansible_ssh_common_args='-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null'
  EOT
}

resource "terraform_data" "ansible" {
  depends_on = [
    azurerm_linux_virtual_machine.vm_streaming,
    local_file.inventario,
  ]

  # Si cambia la VM, el playbook, el compose, el .env o cualquier archivo del
  # proyecto, Terraform vuelve a ejecutar Ansible en el siguiente apply
  triggers_replace = concat(
    [
      azurerm_linux_virtual_machine.vm_streaming.id,
      filemd5("${path.module}/../ansible/playbook.yml"),
      filemd5("${path.module}/../docker-compose.yml"),
      filemd5("${path.module}/../.env"),
    ],
    [for f in sort(fileset("${path.module}/..", "{kafka,productores,flink,telegraf,grafana}/**")) :
      filemd5("${path.module}/../${f}")]
  )

  provisioner "local-exec" {
    working_dir = "${path.module}/../ansible"
    command     = "ansible-playbook -i inventory.ini playbook.yml"
    environment = {
      ANSIBLE_HOST_KEY_CHECKING = "False"
      ANSIBLE_FORCE_COLOR       = "1"
    }
  }
}
