# -- mode: ruby --
# vi: set ft=ruby :

Vagrant.configure("2") do |config|
  config.vm.boot_timeout = 600

  if Vagrant.has_plugin? "vagrant-vbguest"
    config.vbguest.no_install = true
    config.vbguest.auto_update = false
    config.vbguest.no_remote = true
  end

  config.vm.define :controlNode do |controlNode|
    controlNode.vm.box = "bento/ubuntu-22.04"
    controlNode.vm.network :private_network, ip: "192.168.100.5"
    controlNode.vm.hostname = "control-node"
    controlNode.vm.provider "virtualbox" do |v|
      v.name = "control-node"
      v.cpus = 2
      v.memory = 2048
    end
    controlNode.vm.provision "shell", path: "script.sh"
  end
end