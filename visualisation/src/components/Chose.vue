<template>
  <div class="chose">
    <md-whiteframe>
      <h1 class="md-title">
        Rues du Grand Lyon
      </h1>
      <md-button class="md-raised" v-on:click="calculer">
        Lancer !
      </md-button>
      <p>
        {{ statut }}
      </p>
      <md-button class="md-raised" v-on:click="afficher">
        Rafraîchir
      </md-button>
      <p>
        Les résultats :
      </p>
      <md-list class="md-double-line">
        <md-list-item v-for="item in results">
          <div class="md-body-2" style="text-align: center; margin-right: 15px;">
            {{ item.index }}
          </div>
          <div class="md-list-text-container">
            <span>{{ item.nom }}</span>
            <span>{{ item.rang }}</span>
          </div>
        </md-list-item>
      </md-list>

    </md-whiteframe>
  </div>
</template>

<script>
export default {
  name: 'chose',
  data () {
    return {
      statut: 'L’application n’a pas encore été lancée.',
      results: [],
    }
  },
  methods: {
    calculer () {
      this.statut = 'Calcul en cours...'
      this.$http.get('launch')
        .then(() => {
          this.statut = 'Calcul terminé !'
          this.afficher()
        })
        .catch(() => {
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
  .chose {
  padding: 5px;
  }
</style>
