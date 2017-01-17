<template>
  <div id="app">
    <titre></titre>
    <div id="content">
      <p>
        Voici une utilisation possible de <a href="https://hadoop.apache.org/">Hadoop</a> utilisant l’algorithme <a href="https://fr.wikipedia.org/wiki/MapReduce">MapReduce</a>.
      </p>
      <p>
        Des données concernant les rues du Grand Lyon ont été obtenues sur le <a href="https://download.data.grandlyon.com/catalogue/srv/fr/catalog.search#/home">Catalogue du Grand Lyon</a>, et utilisées dans un <a href="https://fr.wikipedia.org/wiki/PageRank">PageRank</a>, afin de les classer par « popularité ».
      </p>
      <chose></chose>
    </div>
  </div>
</template>

<script>
import Titre from './components/Titre'
import Chose from './components/Chose'

export default {
  name: 'app',
  components: {
    Titre,
    Chose,
  }
}
</script>

<style>
#content {
  padding: 5px;
}

</style>
