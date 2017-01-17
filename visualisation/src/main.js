import Vue from 'vue'
import VueResource from 'vue-resource'
import VueMaterial from 'vue-material'
import 'vue-material/dist/vue-material.css'
import App from './App'

Vue.use(VueResource)
Vue.use(VueMaterial)

Vue.http.options.root = 'http://localhost:5000'

/* eslint-disable no-new */
new Vue({
  el: '#app',
  template: '<App/>',
  components: { App }
})
