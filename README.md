# Practica 1: WordCount concurrent

Un unic projecte IntelliJ, basat en el codi sequencial proporcionat. S'ha treballat amb OpenJDK 23.0.2, sense biblioteques Java externes.

## Executar

A IntelliJ, executar `WordCount` amb aquesta carpeta com a directori de treball. Exemple d'arguments:

```text
./Test/test1.txt ./Output/Test1 3 4
```

Els arguments son entrada, prefix de sortida, nombre de reducers i nombre de fils Map. Nomes l'entrada es obligatoria. Per defecte s'utilitzen `./result`, un reducer i un fil Map. L'entrada pot ser un fitxer o una carpeta amb subcarpetes. Les sortides son `prefix.r1` fins a `prefix.rN` i queden ordenades per paraula.

Per compilar des de PowerShell:

```powershell
.\compila.ps1
.\proves.ps1
```

Els scripts detecten el JDK 23 de l'usuari, `JAVA_HOME` o el JDK del PATH. Tambe accepten `-JdkPath 'C:\ruta\del\jdk'`. La compilacio genera `out/classes` i `out/WordCount.jar`. Per executar el JAR, utilitzar el mateix JDK amb que s'ha compilat:

```powershell
& "$env:USERPROFILE\.jdks\openjdk-23.0.2\bin\java.exe" -jar out/WordCount.jar ./Test/ ./Output/TestTots 4 4
```

## Funcionament

```text
Split (fils virtuals) -> cues de linies -> Map (pool fix)
    -> cues de tuples -> Shuffle (pool fix)
    -> esperar les tres etapes -> Reduce (fils normals) -> fitxers
```

- Un Split i un Map per fitxer. El Map consumeix linies mentre el Split encara llegeix.
- Cada Split envia una tupla amb clau negativa despres de l'ultima linia. Una cua temporalment buida no significa que s'hagi acabat.
- El pool de Maps te el nombre de fils indicat per l'usuari. Cada Map retorna el seu recompte, que suma el coordinador.
- Cada paraula va a un Shuffle segons el hash. Hi ha un Shuffle i un reducer per particio. Un fitxer de sortida buit pot ser correcte.
- Un comptador atomic inclou els Maps actius i els pendents del pool. Shuffle acaba quan aquest comptador es zero i la seva cua es buida.
- Els reducers es posen en marxa quan les tres etapes anteriors han acabat. Cadascun escriu un fitxer propi i te un recompte local de paraules diferents.

S'utilitzen les eines dels apunts: `Runnable`, `Thread`, fils virtuals, `join()`, `Callable`, `Future`, pools fixos i col·leccions concurrents. Els pools es tanquen amb `shutdown`/`shutdownNow` i s'esperen amb `awaitTermination`. No s'han afegit locks, semafors ni blocs `synchronized`.

Les cues estan limitades: 256 linies per Split i 4096 tuples per Shuffle. Si alguna tasca falla, ho comunica per una cua concurrent d'errors. La resta de tasques comproven aquest canal i les interrupcions. El coordinador cancel·la i espera tots els fils abans de retornar, tanca els fitxers i elimina les sortides parcials. Nomes la CLI decideix el codi de sortida del proces.

La llista de valors agrupats continua creixent amb les paraules processades, com en el codi base. No es processen enllaços simbolics per evitar cicles durant el recorregut.

## Comprovacions

`proves.ps1` executa `tests/ComprovaPractica.java`, sense JUnit. Comprova els recomptes amb diverses combinacions de fils i reducers, accents, fitxers i carpetes buides, subcarpetes, reutilitzacio dels comptadors, particio amb hash minim, arguments invalids, errors de les tasques i de sortida, proteccio de l'entrada i cancel·lacio. Tambe comprova el limit del pool, el solapament real de les etapes i que no quedin fils vius.

Les capçaleres dels fonts inclouen els noms complets i els NIF dels dos autors.
