:: Disabilita IPv6 per tutte le interfacce di rete
echo net.ipv6.conf.all.disable_ipv6 = 1 >> /etc/sysctl.conf
:: Disabilita IPv6 per le interfacce di rete predefinite
echo net.ipv6.conf.default.disable_ipv6 = 1 >> /etc/sysctl.conf
:: Applica le modifiche apportate a sysctl.conf
sysctl -p
 
:: Disabilita SELinux modificando il file di configurazione
sed -i 's/SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config
 
:: Arresta il servizio firewalld
systemctl stop firewalld
:: Disabilita il servizio firewalld dall'avvio automatico
systemctl disable firewalld
:: Maschera il servizio firewalld per impedirne l'avvio manuale
systemctl mask --now firewalld
 
:: Imposta l'orologio di sistema per utilizzare UTC
:: invece del fuso orario locale
:: Utile per la coerenza nei sistemi distribuiti
timedatectl set-local-rtc 0
 
:: Mostra lo stato della sincronizzazione NTP
ntpq -p
 
:: Riduce il valore di swappiness a 1
:: Questo minimizza l'uso dello spazio di swap
:: e dà priorità all'uso della RAM
echo "vm.swappiness = 1" >> /etc/sysctl.conf
 
:: Aumenta il numero massimo di aree di mappatura della memoria
:: Utile per applicazioni che richiedono
:: un gran numero di mappature di memoria
echo "vm.max_map_count = 8000000" >> /etc/sysctl.conf
 
:: Imposta la directory /tmp con il bit sticky
:: Questo garantisce che solo il proprietario di un file possa eliminarlo
chmod 1777 /tmp
 
wget https://files-cdn.liferay.com/mirrors/download.oracle.com/otn-pub/java/jdk/8u121-b13/jdk-8u121-linux-x64.rpm
 
wget https://files.liferay.com/mirrors/download.oracle.com/otn-pub/java/jdk/8u121-b13/jdk-8u121-linux-x64.rpm
 
Scusatemi ho una chiamata ci aggiorniamo alle 11.55
 
wget http://download.oracle.com/otn-pub/java/jce/8/jce_policy-8.zip
Unauthorized Request
 
/usr/java/jdk1.8.0_121/jre/lib/security/
 
mkfs.xfs -f /dev/sda1
