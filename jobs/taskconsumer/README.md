# Query Stream

New:
 Ha bisogno di un consumer group Il consumer group gli serve perche' crea una lease sulla partizione per consumer group. 
 Ordinamento per partizione e object-id

Esegue una find di una partizione mongo, e conta le partizioni.
le partizioni sono per collection. 
   bid: collection:001
   _gid: collection
   _et: mongo-partition
   np: int
   
array of partitions

ho la lista delle partizioni
ho fatto uno shuffle delle partizioni.

next:
   debbo acquisire una partizione?
      acquisiscila --> non sono riuscito --> errore 
      inizializza il batch con il filtro e la lease.
   
   
   Se non ce ne e' nessuna disponibile esce.

   Se ce ne e' una prova ad acquisirla
      Prova ad acquisirla creando una lease sulla partizione con quel consumer-group come lease group
      Se non ci riesce passa alla successiva se sono finite esce
      Se ci riesce esegue un query di un batch se ha roba OK
      se no prende la successiva

   a questo punto ha una partizione e sa che puo' farci qualcosa 
   scansiona i documenti ordinati per _id crescente
     --> ritorna il documento.
     al commit salva l'object-id nella lease.... e magari la rinnova pure?

rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })

  
  
```yaml
query-stream:
  consumer:
    instance: default
    collection-id: consumer-group
    pid: whatever
    gid: consumer-grp-id

  query-source:
    instance: default
    collection-id: my-source-collection
    batch-size: 100
```
 
 