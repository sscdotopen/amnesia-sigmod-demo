var initRequest = '{"change":"Add", "interactions":[[0,1], [0,2], [1,0], [1,1], [1,3], [2,1], [2,3]]}';

var socket = new WebSocket("ws://" + window.location.host + "/ws");

socket.onmessage = function (event) {
  var messages = document.getElementById("messages");
  messages.append(event.data + "\n");

  var change = JSON.parse(event.data);

  if (change['data'] == 'item_interactions_n' && change['change'] == -1) {
    var item = change['item'];
    var count = change['count'];
    document.getElementById("interactions_per_item_" + item).innerHTML = "-";
  }

  if (change['data'] == 'item_interactions_n' && change['change'] == 1) {
    var item = change['item'];
    var count = change['count'];
    document.getElementById("interactions_per_item_" + item).innerHTML = count;
  }

  if (change['data'] == 'cooccurrences_c' && change['change'] == -1) {
    var item_a = change['item_a'];
    var item_b = change['item_b'];
    var count = change['num_cooccurrences'];
    document.getElementById("cooccurrences_" + item_a + "_" + item_b).innerHTML = "-";
    document.getElementById("cooccurrences_" + item_b + "_" + item_a).innerHTML = "-";
  }


  if (change['data'] == 'cooccurrences_c' && change['change'] == 1) {
    var item_a = change['item_a'];
    var item_b = change['item_b'];
    var count = change['num_cooccurrences'];
    document.getElementById("cooccurrences_" + item_a + "_" + item_b).innerHTML = count;
    document.getElementById("cooccurrences_" + item_b + "_" + item_a).innerHTML = count;
  }

  if (change['data'] == 'similarities_s' && change['change'] == -1) {
    var item_a = change['item_a'];
    var item_b = change['item_b'];
    var similarity = change['similarity'];
    document.getElementById("similarities_" + item_a + "_" + item_b).innerHTML = "-";
    document.getElementById("similarities_" + item_b + "_" + item_a).innerHTML = "-";
  }

  if (change['data'] == 'similarities_s' && change['change'] == 1) {
    var item_a = change['item_a'];
    var item_b = change['item_b'];
    var similarity = change['similarity'];
    document.getElementById("similarities_" + item_a + "_" + item_b).innerHTML = Number(similarity).toFixed(2);
    document.getElementById("similarities_" + item_b + "_" + item_a).innerHTML = Number(similarity).toFixed(2);
  }

  if (change['data'] == 'recommendations' && change['change'] == 1) {
    var query = change['query'];
    var item = change['item'];

    document.getElementById("recommendation_" + query).innerHTML = item;
  }

};

var form = document.getElementById("form");
form.addEventListener('submit', function (event) {
  event.preventDefault();
  var input = document.getElementById("msg");
  socket.send(input.value);
  input.value = "";
});