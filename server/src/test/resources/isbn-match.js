x => {
  var identifiers = JSON.parse(x).identifiers;
  const isbn = [];
  for (i = 0; i < identifiers.length; i++) {
    isbn.push(identifiers[i].isbn);
  };
  return isbn;
}
