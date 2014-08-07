require 'geocoder/sql'
require 'geocoder/stores/base'
require 'pry'

module Geocoder::Store
  module DataMapper
    include Base


    def bearing=(value)
      @bearing = value.to_f
    end

    def bearing
      @bearing
    end

    def distance=(value)
      @distance = value.to_f
    end

    def distance
      @distance
    end

    ##
    # Implementation of 'included' hook method.
    #
    def self.included(base)
      base.extend ClassMethods
    end

    ##
    # Methods which will be class methods of the including class.
    #
    module ClassMethods
      # scope: geocoded objects
      def geocoded
        all(geocoder_options[:latitude].not =>  nil, geocoder_options[:longitude].not => nil)
      end

      # define_method: not-geocoded objects
      def not_geocoded
        all(geocoder_options[:latitude] => nil) + all(geocoder_options[:longitude] => nil)
      end

      ##
      # Find all objects within a radius of the given location.
      # Location may be either a string to geocode or an array of
      # coordinates (<tt>[lat,lon]</tt>). Also takes an options hash
      # (see Geocoder::Store::ActiveRecord::ClassMethods.near_scope_options
      # for details).
      #
      define_method :near do |location, *args|
        latitude, longitude = Geocoder::Calculations.extract_coordinates(location)
        if Geocoder::Calculations.coordinates_present?(latitude, longitude)
          options = near_scope_options(latitude, longitude, *args)

          # conditions ist ein Array, in dem am Anfang die Bedingungen als SQL-String liegen und dann
          # die einzelnen Parameter in den Bedingungen folgen. Daher wird das jetzt auseinandergenommen.
          query_params = options[:conditions]
          conditions = query_params.slice!(0)
          # conditions enthält nun die Bedingung als String, und query_params sind nur noch die zu setzenden
          # parameter der Query

          by_sql do |m|
            [ "SELECT #{select_fields(m, options[:select])} FROM #{m} WHERE #{conditions} ORDER BY #{options[:order]}",
              *query_params]
          end



          #select(options[:select]).where(options[:conditions]).
          #    order(options[:order])
        else
          # If no lat/lon given we don't want any results, but we still
          # need distance and bearing columns so you can add, for example:
          # .order("distance")
          by_sql do |m|
            "SELECT #{select_fields(m, select_clause(nil, "NULL", "NULL"))} FROM #{m} WHERE #{false_condition}"
          end
          #select(select_clause(nil, "NULL", "NULL")).where(false_condition)
        end
      end


      ##
      # Find all objects within the area of a given bounding box.
      # Bounds must be an array of locations specifying the southwest
      # corner followed by the northeast corner of the box
      # (<tt>[[sw_lat, sw_lon], [ne_lat, ne_lon]]</tt>).
      #
      define_method :within_bounding_box, lambda{ |bounds|
        sw_lat, sw_lng, ne_lat, ne_lng = bounds.flatten if bounds
        if sw_lat && sw_lng && ne_lat && ne_lng
          cond = Geocoder::Sql.within_  bounding_box(
                        sw_lat, sw_lng, ne_lat, ne_lng,
                        full_column_name(geocoder_options[:latitude]),
                        full_column_name(geocoder_options[:longitude])
                    )
          by_sql do |m|
            "SELECT #{m}.* FROM #{m} WHERE #{cond}"
          end
          #where(Geocoder::Sql.within_bounding_box(
          #          sw_lat, sw_lng, ne_lat, ne_lng,
          #          full_column_name(geocoder_options[:latitude]),
          #          full_column_name(geocoder_options[:longitude])
          #      ))
        else
          by_sql do |m|
            "SELECT #{select_fields(select_clause(nil, "NULL", "NULL"))} FROM #{m} WHERE #{false_condition}"
          end
          #select(select_clause(nil, "NULL", "NULL")).where(false_condition)
        end
      }
      

      def distance_from_sql(location, *args)
        latitude, longitude = Geocoder::Calculations.extract_coordinates(location)
        if Geocoder::Calculations.coordinates_present?(latitude, longitude)
          distance_sql(latitude, longitude, *args)
        end
      end

      ## Get the Primary Key from the Resource
      def primary_key
        raise "You can'tdo geo-queries with DataMapper on objects with compound primary keys." if key.count > 1
        key.first.field
      end

      private # ----------------------------------------------------------------

      def by_sql
        case sql_or_query = yield(storage_name)
          when Array
            sql, *bind_values = sql_or_query
          when String
            sql, bind_values = sql_or_query, []
        end


        records = []
        geo_data = []

        repository.adapter.send(:with_connection) do |connection|
          reader = connection.create_command(sql).execute_reader(*bind_values)
          fields = properties.field_map.values_at(*reader.fields).compact


          begin
            while reader.next!
              records << Hash[ fields.zip(reader.values) ]
              geo_data << reader.values(-2..-1)

            end
          ensure
            reader.close
          end
        end

        binding.pry
        query = ::DataMapper::Query.new(repository, self, :fields => properties, :reload => false)

        c = ::DataMapper::Collection.new(query, query.model.load(records, query))
        c.each_with_index do |item, index|
          item.distance = geodata[index][0]
          item.bearing = geodata[index][1]
        end
        binding.pry
        c
      end

      ##
      # Get options hash suitable for passing to ActiveRecord.find to get
      # records within a radius (in kilometers) of the given point.
      # Options hash may include:
      #
      # * +:units+   - <tt>:mi</tt> or <tt>:km</tt>; to be used.
      #   for interpreting radius as well as the +distance+ attribute which
      #   is added to each found nearby object.
      #   Use Geocoder.configure[:units] to configure default units.
      # * +:bearing+ - <tt>:linear</tt> or <tt>:spherical</tt>.
      #   the method to be used for calculating the bearing (direction)
      #   between the given point and each found nearby point;
      #   set to false for no bearing calculation. Use
      #   Geocoder.configure[:distances] to configure default calculation method.
      # * +:select+          - string with the SELECT SQL fragment (e.g. “id, name”)
      # * +:select_distance+ - whether to include the distance alias in the
      #                        SELECT SQL fragment (e.g. <formula> AS distance)
      # * +:select_bearing+  - like +:select_distance+ but for bearing.
      # * +:order+           - column(s) for ORDER BY SQL clause; default is distance;
      #                        set to false or nil to omit the ORDER BY clause
      # * +:exclude+         - an object to exclude (used by the +nearbys+ method)
      # * +:distance_column+ - used to set the column name of the calculated distance.
      # * +:bearing_column+  - used to set the column name of the calculated bearing.
      # * +:min_radius+      - the value to use as the minimum radius.
      #                        ignored if database is sqlite.
      #                        default is 0.0
      #
      def near_scope_options(latitude, longitude, radius = 20, options = {})
        if options[:units]
          options[:units] = options[:units].to_sym
        end
        options[:units] ||= (geocoder_options[:units] || Geocoder.config.units)
        select_distance = options.fetch(:select_distance, true)
        options[:order] = "" if !select_distance && !options.include?(:order)
        select_bearing = options.fetch(:select_bearing, true)
        bearing = bearing_sql(latitude, longitude, options)
        distance = distance_sql(latitude, longitude, options)
        distance_column = options.fetch(:distance_column, 'distance')
        bearing_column = options.fetch(:bearing_column, 'bearing')

        b = Geocoder::Calculations.bounding_box([latitude, longitude], radius, options)
        args = b + [
            full_column_name(geocoder_options[:latitude]),
            full_column_name(geocoder_options[:longitude])
        ]
        bounding_box_conditions = Geocoder::Sql.within_bounding_box(*args)

        if using_sqlite?
          conditions = bounding_box_conditions
        else
          min_radius = options.fetch(:min_radius, 0).to_f
          conditions = [bounding_box_conditions + " AND (#{distance}) BETWEEN ? AND ?", min_radius, radius]
        end
        {
            :select => select_clause(options[:select],
                                     select_distance ? distance : nil,
                                     select_bearing ? bearing : nil,
                                     distance_column,
                                     bearing_column),
            :conditions => add_exclude_condition(conditions, options[:exclude]),
            :order => options.include?(:order) ? options[:order] : "#{distance_column} ASC"
        }
      end

      ##
      # SQL for calculating distance based on the current database's
      # capabilities (trig functions?).
      #
      def distance_sql(latitude, longitude, options = {})
        method_prefix = using_sqlite? ? "approx" : "full"
        Geocoder::Sql.send(
            method_prefix + "_distance",
            latitude, longitude,
            full_column_name(geocoder_options[:latitude]),
            full_column_name(geocoder_options[:longitude]),
            options
        )
      end

      ##
      # SQL for calculating bearing based on the current database's
      # capabilities (trig functions?).
      #
      def bearing_sql(latitude, longitude, options = {})
        if !options.include?(:bearing)
          options[:bearing] = Geocoder.config.distances
        end
        if options[:bearing]
          method_prefix = using_sqlite? ? "approx" : "full"
          Geocoder::Sql.send(
              method_prefix + "_bearing",
              latitude, longitude,
              full_column_name(geocoder_options[:latitude]),
              full_column_name(geocoder_options[:longitude]),
              options
          )
        end
      end

      ##
      # Generate the SELECT clause.
      #
      def select_clause(columns, distance = nil, bearing = nil, distance_column = 'distance', bearing_column = 'bearing')

        if columns == :id_only
          return [ full_column_name(primary_key) ]
        elsif columns == :geo_only
          fields = []
        else
          fields = (columns || properties.field_map.keys.map { |column| full_column_name(column)})
        end

        if distance
          #clause += ", " unless clause.empty?
          #clause += "#{distance} AS #{distance_column}"
          fields << "#{distance} AS #{distance_column}"
        end
        if bearing
          fields << "#{bearing} AS #{bearing_column}"
        end
        fields
      end

      def select_fields(table, fields)
        fields.join(', ')
      end

      ##
      # Adds a condition to exclude a given object by ID.
      # Expects conditions as an array or string. Returns array.
      #
      def add_exclude_condition(conditions, exclude)
        conditions = [conditions] if conditions.is_a?(String)
        if exclude
          conditions[0] << " AND #{full_column_name(primary_key)} != ?"
          conditions << exclude.id
        end
        conditions
      end

      def using_sqlite?
        repository.adapter.options[:adapter].match(/sqlite/i)
      end

      ##
      # Value which can be passed to where() to produce no results.
      #
      def false_condition
        using_sqlite? ? 0 : "false"
      end


      ##
      # Prepend table name if column name doesn't already contain one.
      #
      def full_column_name(column)
        column = column.to_s
        column.include?(".") ? column : [storage_name, column].join(".")
      end
    end

    ##
    # Look up coordinates and assign to +latitude+ and +longitude+ attributes
    # (or other as specified in +geocoded_by+). Returns coordinates (array).
    #
    def geocode
      do_lookup(false) do |o,rs|
        if r = rs.first
          unless r.latitude.nil? or r.longitude.nil?
            o.__send__  "#{self.class.geocoder_options[:latitude]}=",  r.latitude
            o.__send__  "#{self.class.geocoder_options[:longitude]}=", r.longitude
          end
          r.coordinates
        end
      end
    end

    alias_method :fetch_coordinates, :geocode

    ##
    # Look up address and assign to +address+ attribute (or other as specified
    # in +reverse_geocoded_by+). Returns address (string).
    #
    def reverse_geocode
      do_lookup(true) do |o,rs|
        if r = rs.first
          unless r.address.nil?
            o.__send__ "#{self.class.geocoder_options[:fetched_address]}=", r.address
          end
          r.address
        end
      end
    end

    alias_method :fetch_address, :reverse_geocode
  end
end