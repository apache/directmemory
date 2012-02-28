/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
$(function() {
  Wine=function(name,description){
    this.name=name;
    this.description=description;
  }
  clearResultContent=function(){
    $("#result-content" ).html("");
  }
  displayInfo=function(msg){
    //alert(msg);
    clearResultContent();
    $("#result-content" ).html($("#alert-message-info").tmpl({message:msg}));
  }

  displayError=function(msg){
    //alert(msg);
    clearResultContent();
    $("#result-content" ).html($("#alert-message-error").tmpl({message:msg}));
  }

  displayWarning=function(msg){
    //alert(msg);
    clearResultContent();
    $("#result-content" ).html($("#alert-message-warning").tmpl({message:msg}));
  }

  // X-DirectMemory-ExpiresIn
  putWineInCache=function(wine,expiresIn){
    $.ajax({
      url: 'cache/'+encodeURIComponent(wine.name),
      data:$.toJSON( wine ),
      cache: false,
      type: 'PUT',
      headers:{'X-DirectMemory-ExpiresIn':expiresIn},
      contentType: "text/plain",
      statusCode: {
        204: function() {
          displayWarning("not put in cache");
        },
        200:function( data ) {
          displayInfo('put in cache with key:'+wine.name);
        },
        500:function(data){
          displayError("error put in cache");
        }
      }
    });
  }

  deleteFromCache=function(key){
    $.ajax({
      url: 'cache/'+encodeURIComponent(key),
      cache: false,
      type: 'DELETE',
      dataType: 'text',
      statusCode: {
        204: function() {
          displayWarning("not found in cache");
        },
        200:function( data ) {
          displayInfo(' key '+key+ ' deleted from cache');
        },
        500:function(data){
          displayError("error delete from cache");
        }
      }
    });
  }

  $(document).ready(function() {

    $("#put-cache-btn").on('click',function(){
      var wine = new Wine($("#wine_name" ).val(),$("#wine_description" ).val());
      if ( $.trim(wine.name ).length<1){
        displayError("name mandatory");
        return;
      }
      var expiresIn=$("#expires-in").val()?$("#expires-in").val():0;
      if( $.trim(expiresIn).length>0 && !$.isNumeric($.trim(expiresIn))){
        displayError("expiresIn must be a number");
        return;
      }
      putWineInCache(wine,expiresIn);

    });

    $("#get_cache_btn").on('click',function(){
      var key = $("#wine_name_cache" ).val();
      if ( $.trim(key).length<1){
        displayError("key mandatory");
        return;
      }

      $.ajax({
        url: 'cache/'+encodeURIComponent(key),
        cache: false,
        type: 'GET',
        dataType: 'text',
        statusCode: {
          204: function() {
            displayWarning("not found in cache");
          },
          200:function( data ) {
            var wine = $.parseJSON(data);
            displayInfo('get from cache with key:'+wine.description);
          },
          500:function(data){
            displayError("error get from cache");
          }
        }
      });

    });

    $("#delete_cache_btn").on('click',function(){
      var key = $("#wine_key_cache" ).val();
      if ( $.trim(key).length<1){
        displayError("key mandatory");
        return;
      }
      deleteFromCache(key);
    });


  });


});