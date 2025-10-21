
function cancerDesc() {
    document.getElementById("testDesc").innerHTML = "Cancer is a group of diseases involving abnormal cell growth with the potential to invade or spread to other parts of the body. Possible signs and symptoms include a lump, abnormal bleeding, prolonged cough, unexplained weight loss, and a change in bowel movements.";
}

function heartDesc() {
    document.getElementById("testDesc").innerHTML = "Heart disease describes a range of conditions that affect your heart. Diseases under the heart disease umbrella include blood vessel diseases, such as coronary artery disease; heart rhythm problems (arrhythmias); and heart defects you're born with (congenital heart defects), among others.";
}

function strokeDesc() {
    document.getElementById("testDesc").innerHTML = "A stroke is a medical condition in which poor blood flow to the brain results in cell death. There are two main types of stroke: ischemic, due to lack of blood flow, and hemorrhagic, due to bleeding. Both result in parts of the brain not functioning properly.";
}
    
function pneumoniaDesc() {
    document.getElementById("testDesc").innerHTML = "Pneumonia is an inflammatory condition of the lung affecting primarily the small air sacs known as alveoli. Typically symptoms include some combination of productive or dry cough, chest pain, fever, and difficulty breathing.";
}

function openOptions(optionName,elmnt) {
    var i, tabcontent, tablinks;
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }
    tablinks = document.getElementsByClassName("tablink");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].style.backgroundColor = "";
    }
    document.getElementById(optionName).style.display = "block";
    elmnt.style.backgroundColor = '#ff9900';
}


