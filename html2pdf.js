#!/usr/bin/env node
const html_to_pdf = require('html-pdf-node');
const path = require('path')

const { exit } = require('process');
// the position starts with 1 instead of 0
if (process.argv.length < 3) {
  console.log("node html2pdf.js <html file|url> <pdf file>");
  exit(1);
}
var source = process.argv[2];
if (! source.startsWith('http')) {
  source = 'file://' + path.resolve(source);
}
const dest = process.argv[3];
let options = { format: 'A4', printBackground: true, path: dest };
// Example of options with args //
// let options = { format: 'A4', args: ['--no-sandbox', '--disable-setuid-sandbox'] };

//let file = { content: "<h1>Welcome to html-pdf-node</h1>" };
// or //
let file = { url: source,}
html_to_pdf.generatePdf(file, options);