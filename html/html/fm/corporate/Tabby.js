function HandleTabs(e,i) {
	if (document.all) { // IE
		tab_to_tab(e, i);
	}
	else { // Standards-compliance mode; more powerful
		checkTab(e);
	}
}

function tab_to_tab(e,el) {
    //A function to capture a tab keypress in a textarea and insert 4 spaces and NOT change focus.
    //9 is the tab key, except maybe it's 25 in Safari? oh well for them ...
    if (e.keyCode==9){
        var oldscroll = el.scrollTop; //So the scroll won't move after a tabbing
        e.returnValue=false;  //This doesn't seem to help anything, maybe it helps for IE
        //Check if we're in a firefox deal
      	if (el.setSelectionRange) {
      	    var pos_to_leave_caret=el.selectionStart+4;
      	    //Put in the tab
     	    el.value = el.value.substring(0,el.selectionStart) + '    ' + el.value.substring(el.selectionEnd,el.value.length);
            //There's no easy way to have the focus stay in the textarea, below seems to work though
            setTimeout("var t=document.getElementById('code1'); t.focus(); t.setSelectionRange(" + pos_to_leave_caret + ", " + pos_to_leave_caret + ");", 0);
      	}
      	//Handle IE
      	else {
      		// IE code, pretty simple really
      		document.selection.createRange().text='    ';
      	}
        el.scrollTop = oldscroll; //put back the scroll
    }
}
        
        function checkTab(evt) {
        	var tab = "    ";
            var t = evt.target;
            var ss = t.selectionStart;
            var se = t.selectionEnd;

            // Tab key - insert tab expansion
            if (evt.keyCode == 9) {
                evt.preventDefault();
                
                // Special case of multi line selection
                if (ss != se && t.value.slice(ss,se).indexOf("\n") != -1) {
                    // In case selection was not of entire lines (e.g. selection begins in the middle of a line)
                    // we ought to tab at the beginning as well as at the start of every following line.
                    var pre = t.value.slice(0,ss);
                    var sel = t.value.slice(ss,se).replace(/\n/g,"\n"+tab);
                    var post = t.value.slice(se,t.value.length);
                    t.value = pre.concat(tab).concat(sel).concat(post);
                    
                    t.selectionStart = ss + tab.length;
                    t.selectionEnd = se + tab.length;
                }
                
                // "Normal" case (no selection or selection on one line only)
                else {
                    t.value = t.value.slice(0,ss).concat(tab).concat(t.value.slice(ss,t.value.length));
                    if (ss == se) {
                        t.selectionStart = t.selectionEnd = ss + tab.length;
                    }
                    else {
                        t.selectionStart = ss + tab.length;
                        t.selectionEnd = se + tab.length;
                    }
                }
            }
            
            // Backspace key - delete preceding tab expansion, if exists
            else if (evt.keyCode==8 && t.value.slice(ss - 4,ss) == tab) {
                evt.preventDefault();
                
                t.value = t.value.slice(0,ss - 4).concat(t.value.slice(ss,t.value.length));
                t.selectionStart = t.selectionEnd = ss - tab.length;
            }
            
            // Delete key - delete following tab expansion, if exists
            else if (evt.keyCode==46 && t.value.slice(se,se + 4) == tab) {
                evt.preventDefault();
                
                t.value = t.value.slice(0,ss).concat(t.value.slice(ss + 4,t.value.length));
                t.selectionStart = t.selectionEnd = ss;
            }
            
            // Left/right arrow keys - move across the tab in one go
            else if (evt.keyCode == 37 && t.value.slice(ss - 4,ss) == tab) {
                evt.preventDefault();
                t.selectionStart = t.selectionEnd = ss - 4;
            }
            else if (evt.keyCode == 39 && t.value.slice(ss,ss + 4) == tab) {
                evt.preventDefault();
                t.selectionStart = t.selectionEnd = ss + 4;
            }
            
        }
