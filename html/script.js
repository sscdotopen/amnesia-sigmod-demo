
var socket = new WebSocket("ws://" + window.location.host + "/ws");

var maxUserIdSeen = -1;

function initialize() {
    var initRequest = '{"change":"Add", "interactions":[[0,0], [0,1], [0,2], [1,1], [1,2], [1,4], [2,1], [2,3], [2,4]]}';
    socket.send(initRequest);
    addInteraction(0, 0);
    addInteraction(0, 1);
    addInteraction(0, 2);
    addInteraction(1, 1);
    addInteraction(1, 2);
    addInteraction(1, 4);
    addInteraction(2, 3);
    addInteraction(2, 4);
}

function forget(user) {
    var items = [];

    for (var item=0; item < 5; item++) {
        if (document.getElementById(`interactions_${user}_${item}`).innerHTML == "✔") {
            items.push(item);
        }
    }

    var removeInteractions = [];

    for (var i = 0; i < items.length; i++) {
        removeInteractions.push([user, items[i]]);
    }

    var request = {};
    request["change"] = "Remove";
    request["interactions"] = removeInteractions;

    socket.send(JSON.stringify(request));

    var elem = document.getElementById(`interactions_${user}`);
    elem.parentNode.removeChild(elem);
}

function newUser() {
    var items = [];
    var newInteractions = [];

    for (var item=0; item < 5; item++) {
        if (document.getElementById(`new_${item}`).checked) {
            items.push(item);
            document.getElementById(`new_${item}`).checked = false;
        }
    }

    var userId = maxUserIdSeen + 1;

    if (items.length == 0) {
        alert("Select an item!");
        return;
    }

    for (var i = 0; i < items.length; i++) {
        addInteraction(userId, items[i]);
        newInteractions.push([userId, items[i]]);
    }

    var request = {};
    request["change"] = "Add";
    request["interactions"] = newInteractions;

    socket.send(JSON.stringify(request));
}

function addInteraction(user, item) {

    if (user > maxUserIdSeen) {
        maxUserIdSeen = user;
    }

    if (document.getElementById(`interactions_${user}`) == null) {
        var row = `
            <div class="row" id="interactions_${user}">
                <div id="interactions_${user}_0" class="col-md-2 text-center">-</div>
                <div id="interactions_${user}_1" class="col-md-2 text-center">-</div>
                <div id="interactions_${user}_2" class="col-md-2 text-center">-</div>
                <div id="interactions_${user}_3" class="col-md-2 text-center">-</div>
                <div id="interactions_${user}_4" class="col-md-2 text-center">-</div>
                <div class="col-md-2">
                    <button class="btn btn-xs btn-default" onclick="javascript:forget(${user});">forget</button>
                </div>
            </div>`;
        document.getElementById("interactions").innerHTML += row;
    }

    document.getElementById(`interactions_${user}_${item}`).innerHTML = "✔";

    blink(`#interactions_${user}_${item}`);
}

function blink(elem) {

   var duration = 800;

    $(elem)
        .fadeOut(duration).fadeIn(duration)
        .fadeOut(duration).fadeIn(duration)
        .fadeOut(duration).fadeIn(duration);
}


socket.onmessage = function (event) {
  var messages = document.getElementById("messages");
  messages.innerHTML = event.data + "\n" + messages.innerHTML;

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
    blink(`#interactions_per_item_${item}`);
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
    blink(`#cooccurrences_${item_a}_${item_b}`);
    blink(`#cooccurrences_${item_b}_${item_a}`);
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
    blink(`#similarities_${item_a}_${item_b}`);
    blink(`#similarities_${item_b}_${item_a}`);
  }

  if (change['data'] == 'recommendations' && change['change'] == -1) {
    var query = change['query'];
    document.getElementById("recommendation_" + query).innerHTML = "";
  }

  if (change['data'] == 'recommendations' && change['change'] == 1) {
    var query = change['query'];
    var item = change['item'];

    var images = [
        "https://m.media-amazon.com/images/M/MV5BNDUxN2I5NDUtZjdlMC00NjlmLTg0OTQtNjk0NjAxZjFmZTUzXkEyXkFqcGdeQXVyMTQxNzMzNDI@._V1_UX182_CR0,0,182,268_AL_.jpg",
        "https://m.media-amazon.com/images/M/MV5BNDUxN2I5NDUtZjdlMC00NjlmLTg0OTQtNjk0NjAxZjFmZTUzXkEyXkFqcGdeQXVyMTQxNzMzNDI@._V1_UX182_CR0,0,182,268_AL_.jpg",
        "https://m.media-amazon.com/images/M/MV5BZDAwYTlhMDEtNTg0OS00NDY2LWJjOWItNWY3YTZkM2UxYzUzXkEyXkFqcGdeQXVyNTA4NzY1MzY@._V1_UY268_CR4,0,182,268_AL_.jpg",
        "https://m.media-amazon.com/images/M/MV5BNGEwMTRmZTQtMDY4Ni00MTliLTk5ZmMtOWMxYWMyMTllMDg0L2ltYWdlL2ltYWdlXkEyXkFqcGdeQXVyNjc1NTYyMjg@._V1_UX182_CR0,0,182,268_AL_.jpg",
        "https://m.media-amazon.com/images/M/MV5BMDdmZGU3NDQtY2E5My00ZTliLWIzOTUtMTY4ZGI1YjdiNjk3XkEyXkFqcGdeQXVyNTA4NzY1MzY@._V1_UX182_CR0,0,182,268_AL_.jpg"
    ];

    document.getElementById("recommendation_" + query).innerHTML = `<img class="query-image" src="${images[item]}"/>`;

    blink(`#recommendation_${query}`);
  }

};
