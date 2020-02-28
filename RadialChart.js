class RadialChart extends Chart {
    constructor() {
        super("bar");
    }

    display(image, randomValues, randomIndices) {     
    var width = 300,
    height = 300,
    barHeight = height / 2 - 40,
    radius = Math.min(width, height) / 2;

var formatNumber = d3.format("s");
var color = d3.scaleOrdinal()

var svg = image.append("g")
    .attr("transform", "translate(" + width/2 + "," + height/2 +")");

   var data = d3.pie().sort(null)
        .value(function(d) { return d; })(randomValues);

  //data.sort(function(a,b) { return b.value - a.value; });

  var extent = d3.extent(data, function(d) { return d.value; });
  var barScale = d3.scaleLinear()
      .domain(extent)
      .range([0, barHeight]);

  var keys = ['','','','','','','','','',''];
  var numBars = keys.length;



  var labels;
        var Apos = Math.ceil(Math.random()*9);
        var Bpos = Math.ceil(Math.random()*9);
        while(Apos==Bpos){
            Bpos = Math.ceil(Math.random()*9);
        }
        labels = ['', '', '', '', '', '', '', '', '', ''];
        labels[Apos]=".";
        labels[Bpos]=".";


  var x = d3.scaleLinear()
      .domain(extent)
      .range([0, -barHeight]);

  var xAxis = d3.axisLeft()
      
      .ticks(3)
      .tickFormat(formatNumber);
      
  var circles = svg.selectAll("circle")
          .data(x.ticks(3))
        .enter().append("circle")
          .attr("r", function(d) {return barScale(d);})
          .style("fill", "none")
          .style("stroke", "black")
          .style("stroke-dasharray", "2,2")
          .style("stroke-width",".5px");

var arc = d3.arc()
      .startAngle(function(d,i) { return (i * 2 * Math.PI) / numBars; })
      .endAngle(function(d,i) { return ((i + 1) * 2 * Math.PI) / numBars; })
      .innerRadius(0)
      .outerRadius(0);


      
      var segments = svg.selectAll("path")
          .data(data)
        .enter().append("path")
          .each(function(d) { d.outerRadius = 0; })
          
          .attr("d", arc);

  svg.append("circle")
      .attr("r", barHeight)
      .classed("outer", true)
      .style("fill", "none")
      .style("stroke", "black")
      .style("stroke-width","1.5px");

  var lines = svg.selectAll("line")
      .data(keys)
    .enter().append("line")
      .attr("y2", -barHeight - 20)
      .style("stroke", "black")
      .style("stroke-width",".5px")
      .attr("transform", function(d, i) { return "rotate(" + (i * 360 / numBars) + ")"; });

  var labelRadius = barHeight * 1.025;

  var labels = svg.append("g")
      .classed("labels", true);

  labels.append("def")
        .append("path")
        .attr("id", "label-path")
        .attr("d", "m0 " + -labelRadius + " a" + labelRadius + " " + labelRadius + " 0 1,1 -0.01 0");

var point = svg.append("svg:circle")
   .style("fill", "black")
   .attr("r", 2)
   .attr("cx", Math.ceil(Math.random()*49))
   .attr("cy", Math.ceil(Math.random()*79))

   var point = svg.append("svg:circle")
   .style("fill", "black")
   .attr("r", 2)
   .attr("cx", Math.ceil(Math.random()*-39))
   .attr("cy", Math.ceil(Math.random()*-59))


  labels.selectAll("text")
        .data(keys)
      .enter().append("text")
        .style("text-anchor", "middle")
        .style("font-weight","bold")
        .style("fill", function(d, i) {return "#3e3e3e";})
        .attr("xlink:href", "#label-path")
        .attr("startOffset", function(d, i) {return i * 100 / numBars + 50 / numBars + '%';})
        .append("textPath")
        .text(function(d) {return d.toUpperCase(); });

}
}
        
    

