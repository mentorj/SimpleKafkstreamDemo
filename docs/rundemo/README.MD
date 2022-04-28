Pour lancer la demo:
- lancer Zookeeper
- lancer Kafka
- creer les 2 topics necessaires a la demo avec les outils CLI Kafka
- lancer Kafka Connect  par l'une des 2 méthodes suivantes:
  - avec les fichiers properties des connecteurs
  - en ajoutant les connecteurs via curl après lancement de Kafka Connect
- lancer le programme Java via maven (client Kafka Stream)
- ajouter du texte au fichier d'input
- tail -f sur le fichier de sortie pour voir la sortie filtrée en live
    