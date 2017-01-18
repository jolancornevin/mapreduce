<template>
  <md-whiteframe class="content">
      <md-button class="md-raised" v-on:click="calculer">
        Lancer !
      </md-button>
      <md-spinner md-indeterminate v-if="running"></md-spinner>
      <md-icon class="md-size-2x" v-if="error">error</md-icon>
      <md-icon class="md-size-2x" v-if="done">done</md-icon>
      <p>
        {{ statut }}
      </p>
      <md-button class="md-raised" v-on:click="afficher">
        Rafraîchir
      </md-button>
      <h1 class="md-title" v-if="done">
        Résultats
      </h1>
      <md-list class="md-double-line">
        <md-list-item v-for="item in results">
          <md-avatar style="text-align: center;">
            {{ item.index }}
          </md-avatar>
          <div class="md-list-text-container">
            <span>{{ item.nom }}</span>
            <span>{{ item.rang }}</span>
          </div>
        </md-list-item>
      </md-list>
  </md-whiteframe>
</template>

<script>
export default {
  name: 'chose',
  data () {
    return {
      statut: 'L’application n’a pas encore été lancée.',
      results: [],
      running: false,
      error: false,
      done: false,
    }
  },
  methods: {
    calculer () {
      this.statut = 'Calcul en cours...'
      this.running = true
      this.error = false
      this.done = false
      this.$http.get('launch')
        .then(() => {
          this.statut = 'Calcul terminé !'
          this.done = true
          this.running = false
          this.afficher()
        })
        .catch(() => {
          this.running = false
          this.error = true
          this.statut = 'Oups, pas de réponse du serveur...'
        })
    },
    afficher () {
      this.$http.get('output')
        .then((response) => {
          this.statut = 'Calcul terminé !'
          let results = response.body
          results.sort((a, b) => a.rang < b.rang)
          this.results = results.map((value, index) => ({...value, index: index + 1}))
        })
        .catch(() => {
          this.statut = 'Oups, pas de réponse du serveur...'
        })
    }
  }
}
</script>

<style scoped>
  .content { padding: 15px; }
</style>
