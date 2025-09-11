mybutton = document.getElementById("myBtn");

// When the user scrolls down 20px from the top of the document, show the button
window.onscroll = function () { scrollFunction() };

function scrollFunction() {
    if (document.body.scrollTop > 20 || document.documentElement.scrollTop > 20) {
        mybutton.style.display = "block";
    } else {
        mybutton.style.display = "none";
    }
}
// When the user clicks on the button, scroll to the top of the document
function topFunction() {
    document.body.scrollTop = 0; // For Safari
    document.documentElement.scrollTop = 0; // For Chrome, Firefox, IE and Opera
}

  function openNav() {
            setTimeout(function () { document.getElementById("overlay").style.display = "block"; }, 300);
        }
/* Close */
 function closeNav() {
          document.getElementById("overlay").style.display = "none";
        }

// popovers initialization - on hover and click
/*
$('[data-toggle="popover-hover"]').popover({
    html: true,
    trigger: 'hover click',
    placement: 'bottom',
    title: function () { return '<p>О ВУЗе</p> <small>(нажмите чтобы открыть или закрыть)</small>'; },
    content: function () {
        return '<p>'+ $(this).data('rektor') + ' </p> <p>Адрес: '
            + $(this).data('address') + ' </p> <p>Сайт: <a target="_blank" href="http://'
            + $(this).data('webpage') + '">' + $(this).data('webpage') + '</a> </p>';
    }
});
*/
function setCookie(cname,cvalue,exdays) {
    var d = new Date();
    d.setTime(d.getTime() + (exdays*24*60*60*1000));
    var expires = "expires=" + d.toGMTString();
    document.cookie = cname + "=" + cvalue + ";" + expires + ";path=/";
  }

function getCookie(cname) {
    var name = cname + "=";
    var decodedCookie = decodeURIComponent(document.cookie);
    var ca = decodedCookie.split(';');
    for(var i = 0; i < ca.length; i++) {
      var c = ca[i];
      while (c.charAt(0) == ' ') {
        c = c.substring(1);
      }
      if (c.indexOf(name) == 0) {
        return c.substring(name.length, c.length);
      }
    }
    return "";
  }
  
function checkCookie() {
    var privacy = getCookie("ORTPORTAL");
    if (privacy != 0) {
        $('#out').show();
     } else {
      $('#out').hide();
    }
  }
 

//$('#out').hide();
//$(document).ready(function () {
  //  checkCookie();
//});  
function language(l) { localStorage.setItem('lang',l);};
window.onstorage = function(e) {if (e.key=="lang") {setCookie('locale',e.newValue, 90)};  };



