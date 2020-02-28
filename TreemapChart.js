
class TreemapChart extends Chart {
    constructor() {
        super("bar");
    }


display(image, randomValues, randomIndices) {
   
var margin = {top: 10, right: 10, bottom: 10, left: 10},
  width = 445 - margin.left - margin.right,
  height = 445 - margin.top - margin.bottom;



  d3.csv('https://raw.githubusercontent.com/aishwarya-shrinivas/wpi/master/treemapdata.csv', 
    function(data) {

var svg = image.append("g")
  .attr("transform",
        "translate(" + margin.left + "," + margin.top + ")");
  

  for(var i=1;i<=10;i++) {
    data[i].value = randomValues[i];
  }


  // stratify the data: reformatting for d3.js
  var root = d3.stratify()
    .id(function(d) { return d.name; })   // Name of the entity (column name is name in csv)
    .parentId(function(d) { return d.parent; })   // Name of the parent (column name is parent in csv)
    (data);

    console.log(data)
    console.log(root)

  root.sum(function(d) {return +d.value })   // Compute the numeric value for each entity

  // Then d3.treemap computes the position of each element of the hierarchy
  // The coordinates are added to the root object above
  d3.treemap()
    .size([width, height])
    .padding(4)
    (root)

console.log('leaves');
console.log(root.leaves())
  // use this information to add rectangles:
  svg
    .selectAll("rect")
    .data(root.leaves())
    .enter()
    .append("rect")
      .attr('x', function (d) { return d.x0; })
      .attr('y', function (d) { return d.y0; })
      .attr('width', function (d) { return d.x1 - d.x0; })
      .attr('height', function (d) { return d.y1 - d.y0; })
      .style("stroke", "black")
      .style("fill", "white");

      var point = svg.append("svg:circle")
   .style("fill", "black")
   .attr("r", 1)
   .attr("cx", 10)
   .attr("cy", 90);

    var point = svg.append("svg:circle")
   .style("fill", "black")
   .attr("r", 1)
   .attr("cx", 200)
   .attr("cy", 400);

  // and to add the text labels
  svg
    .data(root.leaves())
    .enter()
    .append("text")
      .attr("x", function(d){ return d.x0+10})    // +10 to adjust position (more right)
      .attr("y", function(d){ return d.y0+20})    // +20 to adjust position (lower)
      .text(function(d){ return d.data.name})
      .attr("font-size", "15px")
      .attr("fill", "white")
})



}
}
