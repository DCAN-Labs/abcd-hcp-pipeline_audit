let table = document.querySelector('table');
let columnHeaders = table.querySelector('thead').querySelector('tr').querySelectorAll('th');
let trows = table.querySelector('tbody').querySelectorAll('tr');

// function hideColumn(colNumber) {
//     for (let i=0; i<trows.length; i++) {
//         trows[i].children[colNumber].style.display = 'none';
//     }
//     columnHeaders[colNumber].style.display = 'none';

// }

// function showColumn(colNumber) {
//     for (let i=0; i<trows.length; i++) {
//         trows[i].children[colNumber].style.display = null;
//     }
//     columnHeaders[colNumber].style.display = null;
// }

// function showAllColumns(){
//     for (let i = 0; i < columnHeaders.length; i++) {
//         showColumn(i);        
//     }
// }

function showAllRows() {
    for (let i = 0; i < trows.length; i++) {
        trows[i].style.display = null;
        trows[i].classList = '';
    }
    let boxes = document.querySelectorAll('input');
    for (let i = 0; i < boxes.length; i++) {
        boxes[i].checked = true;
        boxes[i].value = '';
    }
}

function hideRows(colNumber, values) {
    for (let i = 0; i < trows.length; i++) {
        let row = trows[i];
        if (values.includes(row.children[colNumber].innerHTML)) {
            row.style.display = 'none';
            row.classList.add(`hidden-col-${colNumber}`);
        }
    }    
}

function showRows(colNumber, values) {
    for (let i = 0; i < trows.length; i++) {
        let row = trows[i];
        if (values.includes(row.children[colNumber].innerHTML)) {
            let classList = row.classList;
            classList.remove(`hidden-col-${colNumber}`);
            if (!classList.value.match(/.*hidden-col.*/)) {
                row.style.display = null;
            }
        }
    }    
}

function getValues(colNumber) {
    let values = [];
    for (let i = 0; i < trows.length; i++) {
        let value = trows[i].children[colNumber].innerHTML;
        if (!values.includes(value)) {
            values.push(value)
        }
    }
    return values;
}

function addHeaderElement(index, element) {
    columnHeaders[index].innerHTML += element;
}

function createCheckList(title, values, columnNumber) {
    let options = '';
    for (let i = 0; i < values.length; i++) {
        options += (`<li><input name='${columnNumber}' type='checkbox' checked='true'/>${values[i]}</li>`)
    }
    return `<div class='dropdown-check-list'><span class='anchor'>${title}</span><ul class='items'>${options}</ul></div>`;
}

function createSearchBox(title, columnNumber) {
    return `<div class='dropdown-check-list'><span class='anchor'>${title}</span><ul class='items'><li><input id='search${columnNumber}' name='${columnNumber}' type='text' onkeyup='searchBoxFilter(${columnNumber})' placeholder='Type to filter'></li></ul></div>`
}

function searchBoxFilter(columnNumber) {
    let input = document.getElementById(`search${columnNumber}`);
    let filter, td, txtValue;
    filter = input.value.toUpperCase();
    for (let i = 0; i < trows.length; i++) {
        let row = trows[i];
        td = row.children[columnNumber];
        if(td){
        // if (window.getComputedStyle(row).display != 'none') {
            txtValue = td.textContent || td.innerText;
            if (txtValue.toUpperCase().indexOf(filter) > -1) {
                row.classList.remove(`hidden-col-${columnNumber}`);
                if (!row.classList.value.match(/.*hidden-col.*/)) {
                    row.style.display = null;
                }
            } else {
                row.style.display = 'none';
                row.classList.add(`hidden-col-${columnNumber}`);
      }
    }       
  }
}

function addFilters() {
    for (let i = 0; i < columnHeaders.length; i++) {
        let title = 'Filter' //columnHeaders[i].innerHTML;
        let values = getValues(i);
        if (values.length < 5) {
            addHeaderElement(i, createCheckList(title, values, i));
        } else {
            addHeaderElement(i, createSearchBox(title, i));
        }
    }
}

function addSortClicks() {
    for (let i = 0; i < columnHeaders.length; i++) {
        columnHeaders[i].querySelectorAll('div')[1].getElementsByClassName('anchor')[0].addEventListener('click',addSortClick,false);
    }
}

function addSortClick(){
    if (this.parentElement.classList.contains('visible'))
        this.parentElement.classList.remove('visible');
    else
        this.parentElement.classList.add('visible');
}

function columnCheckbox(){
    if (this.checked) {
        showRows(parseInt(this.name), this.parentElement.innerText);
    } else {
        hideRows(parseInt(this.name), this.parentElement.innerText);
    }
}

function addCheckboxEvents() {
    for (let i = 0; i < columnHeaders.length; i++) {
        let boxes = columnHeaders[i].querySelectorAll('input');
        for (let j = 0; j < boxes.length; j++) {
            boxes[j].addEventListener('change',columnCheckbox);
        }
    }
}

function addControls() {
    let control = document.getElementById('control');
    let button = document.createElement('button');
    control.appendChild(button);
    button.outerHTML = `<button type='button' onclick='showAllRows()'>Show All Rows</button>`;
}

function organizeTHs(){
    for (let i = 0; i < columnHeaders.length; i++) {
        let title = columnHeaders[i].innerText;
        if (!title) {
            title = 'Click Column Title to Sort';
        }
        columnHeaders[i].innerHTML = `<div class='title'>${title}</div>`;
    }
}


function init(){
    organizeTHs();
    addFilters();
    addSortClicks();
    addCheckboxEvents();
    addControls();
}

init();