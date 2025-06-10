Ce projet a pour but de détecter des transactions anormales dans l'API publique binance WebSocket, plus spécialement pour les 
cryptomonnaies. 

1. On ingère les données de l'API vers un topic kafka se nommant binance-trades

2. On met les données de binance-trades dans databricks avec un stream dans le premier notebook

3. On stocke ces données dans une delta Table pour pouvoir effectuer un entraînement sur un modèle

4. On charge les données puis on fait du feature engineering pour ensuite utiliser un VectorAssembler pour préparer les données
à l'entraînement

5. On crée un modèle ML utilisant l'agorithme Isolation Forest (modèle tiré de SynapseML) pour détecter les transacions anormales

6. On sauvegarde le modèle et on affiche et sauvegarde les résultats