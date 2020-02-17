# Linux Shell

# Create Directory Output
sudo mkdir /home/hadoop/miniprojeto3/saida

# Start Hadoop and Spark Yarn
start-dfs.sh
start-yarn.sh

# Submit Python app
spark-submit gastosCliente.py

# Visualizing output
cd /home/hadoop/miniprojeto3/saida
nano part-00000