/**
 * TransactionHistogram
 *
 * Object encapsulating the histogram interactions for all transactions. This
 * visualization will show a histogram of the contribution sizes for the list
 * of contributions that the render method is given.
 *
 * @constructor
 * @params {d3.selection} selector
 * @params {d3.dispatch} dispatch
 */
var TransactionHistogram = function (selector, dispatch) {
    this.dispatch = dispatch;
    this.selector = selector;

    // Viz parameters
    this.height = 450;
    this.width = 330;
    var marginBottom = 20;

    // Create histogram svg container
    this.svg = this.selector.append('svg')
        .attr('width', this.width)
        .attr('height', this.height + marginBottom)  // space needed for ticks
        .append('g');

    // Bin counts
    this.bins = [50, 200, 500, 1000, 10000, 50000, 100000, 1000000];
    this.histogramLayout = d3.layout.histogram()
      .bins([0].concat(this.bins))
      .value(function(d) {return d.amount;});

    // Initialize default color
    this.setHistogramColor(this.colorStates.DEFAULT);
};

/**
 * render(data)
 *
 * Given a data list, histogram the contribution amounts. The visualization will
 * show how many contributions were made between a set of monetary ranges defined
 * by the buckets `this.bins`.
 *
 * The data is expected in the following format:
 *     data = [
 *        {'state': 'CA', 'amount': 200},
 *        {'state': 'CA', 'amount': 10},
 *        ...
 *     ]
 *
 * @params {Array} data is an array of objects with keys 'state' and 'amount'
 */
TransactionHistogram.prototype.render = function(data) {
    // Needed to access the component in anonymous functions
    var that = this;

    // Generate the histogram data with D3
    var histogramData = this.histogramLayout(data);

    if (!this.hasScaleSet()) {
        histogramData = this.setScale(data);
    } else {
        histogramData = this.histogramLayout(data);
    }

    /** Histogram visualization setup **/
    // Select the bar groupings and bind to the histogram data
    var bar = this.svg.selectAll('.bar')
          .data(histogramData, function(d) {return d.x;});


    /* Enter phase */
    // Implement

    // Add a new grouping
    if (!bar.enter().empty()){
        var har = bar.enter().append("g").attr("class", "bar");
        har.append("rect");
        har.append("text");
    }
    
    /** Update phase */
    // Implement
    bar.attr("transform", function(d) {return "translate(" + that.xScale(d.x) + "," + 0 + ")"; });
    bar.select('rect')
        .attr("x", 1.66)
        .attr("width", that.xScale(histogramData[0].dx)-1.66)
        .transition().duration(500).attr("height", function(d){return that.yScale(d.y);})
        .attr("y",  function(d){return that.height-that.yScale(d.y);})
        .attr("fill", that.currentColorState);


    bar.select('text').attr("dy", function(d){
        if (that.yScale(d.y) >= 0.03*that.height)
            return 10;
        return -4;
    })
    .transition().duration(490).attr("y",  function(d){return that.height-that.yScale(d.y);})
    .attr("x", that.xScale(histogramData[0].dx) / 2)
    .attr("text-anchor", "middle")
    .attr("font-size", 10)
    .attr("font-family","Verdana")
    .attr("fill", function(d){
        if (that.yScale(d.y) >= 0.03*that.height)
            return "white";
        return "black";
    })
    .text(function(d) {   
        var prefix = d3.formatPrefix(d.y);
        return prefix.scale(d.y).toPrecision(3)+prefix.symbol;
    });

    /** Exit phase */
    // Implement
    bar.exit().remove();
    // Draw / update the axis as well
    this.drawAxis();
};


/**
 * formatBinCount(number)
 *
 * Helper function for formatting the bin count.
 * @params {number} number
 * @return number
 */
TransactionHistogram.prototype.formatBinCount = function (number) {
    if (number < 100)
      return number;
    return d3.format('.3s')(number);
};

/*
 * Helper function for determing the position of text.
 *
 * @params {Object} a single transaction/state object
 * @return {boolean} true if the text should go below the bar
 */
TransactionHistogram.prototype.isBarLargeEnough = function (d) {
    var yCoordinate = (d.y === 0) ? 0 : this.yScale(d.y);
    return (this.height - yCoordinate) > 12;
};


/*
 * drawAxis()
 *
 * Draws the x-axis for the histogram.
 */
TransactionHistogram.prototype.drawAxis = function() {
    // Add the X axis
    var xAxis = d3.svg.axis()
        .scale(this.xScale)
        .orient("bottom")
        .tickFormat(d3.format('$.1s'));

    // Add ticks for xAxis (rendering the xAxis bar and labels)
    if (this.xTicks === undefined) {
      this.xTicks = this.svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + this.height + ")");
    }
    this.xTicks.transition().duration(500).call(xAxis);
};


/*
 * setScale(data)
 *
 * Sets the X and Y scales for the histogram. Based on the `data` provided.
 *
 * @params {Array} data is an array of objects with keys 'state' and 'amount'
 * @return {Array} returns histogramData so that recomputation of buckets not necesary
 */
TransactionHistogram.prototype.setScale = function (data) {
    var histogramData = this.histogramLayout(data);

    this.xScale = d3.scale.threshold()
      .domain(this.bins)
      .range(d3.range(0, this.width, this.width/(this.bins.length + 1)));

    // Implement: define a suitable yScale given the data
    var dataLength = data.length;
    var maxY = -1;
    for (i = 0; i < histogramData.length; i++){
        if (histogramData[i].y > maxY){
            maxY = histogramData[i].y;
        }
    }
    var viewHeight = this.height;
    this.yScale = d3.scale.linear().domain([0, maxY]).range([0,viewHeight]);

    return histogramData;
};

/*
 * hasScaleSet()
 *
 * Checks if the scale has already been set.
 *
 * @return {boolean} true if the x and y scales have been already set.
 */
TransactionHistogram.prototype.hasScaleSet = function () {
    return this.xScale !== undefined && this.yScale !== undefined;
};

TransactionHistogram.prototype.colorStates = {
    'PRIMARY': "orange",
    'SECONDARY': "#0BD90E",
    'DEFAULT': "green"
}


/*
 * setBarColor()
 *
 * Set the color mode of the bar.
 */
TransactionHistogram.prototype.setHistogramColor = function (colorState) {
    this.currentColorState = colorState;
};
