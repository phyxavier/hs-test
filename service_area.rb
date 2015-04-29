require 'acts_as_background_solr'
class ServiceArea < ActiveRecord::Base

  acts_impressionable

  include Liquid::ServiceArea

  defaults :state => "pending", :position => 0.0

  acts_as_state_machine :initial => :pending

  scope :enabled, -> { where(state: 'enabled') }

  belongs_to :company
  belongs_to :city
  belongs_to :primary_contact, :class_name => 'User', :foreign_key => 'contact_user_id'

  has_one  :published_location, :class_name => 'Location', as: :locatable
  has_many :reviews
  has_many :latest_approved_review, :class_name => "Review", :conditions => "reviews.state = 'approved'", :order => "reviews.created_at desc", :include => :user, :limit => 1
  has_many :approved_reviews, :class_name => "Review", :conditions => "reviews.state = 'approved'", :order => "reviews.id", :include => :user do
    def latest(options = {:amount => 1})
      order("reviews.created_at desc").limit(options[:amount])
    end
  end
  has_many :pending_reviews, :class_name => "Review", :conditions => "reviews.state like 'pending%'", :order => "id"
  has_many :approved_or_pending_reviews, :class_name => "Review", :conditions => "reviews.state like 'pending%' OR reviews.state = 'approved'", :order => "reviews.created_at desc"
  has_many :approved_or_investigated_reviews, :class_name => "Review", :conditions => "reviews.state = 'under_investigation' OR reviews.state = 'approved'", :order => "reviews.created_at desc"
  has_many :approved_investigated_approved_and_user_approval_reviews, :class_name => "Review", :conditions => "reviews.state in ('under_investigation','approved','pending_user_confirmation')", :order => "reviews.created_at desc"

  has_many :project_companies

  before_save :record_ruby_call_stack # auditing why, how, company_categories change - tracking a potential bug
  before_update :set_updated_by
  before_create :set_created_by

  validates_presence_of :city

  acts_as_versioned :if_changed => [:city_id, :state, :contact_user_id]

  acts_as_background_solr({:fields => [:name,
                :sort_name,
                :company_name,
                :solr_categories,
                :solr_profile,
                {:solr_position => {:type => :range_float}},
                :solr_first_letter_of_name,
                {:solr_company_id => {:type => :integer}},
                :solr_country,
                {:solr_avg_rating => {:type => :float}},
                # {:solr_number_of_photos => {:type => :integer}},
                {:solr_number_of_photos => {:type => :range_integer}},
                # {:solr_number_of_reviews => {:type => :integer}},
                {:solr_number_of_reviews => {:type => :range_integer}},
                {:solr_listing => {:type => :integer}},
                :solr_city,
                {:solr_company_state => {:type => :string}},
                {:solr_state => {:type => :string}}],
               :include => [{:categories => [:id]}],
               # :facets => [:solr_category_name, :solr_city],
               :boost => Proc.new { |service_area| service_area.boost_me },
               :geographic => [:lat, :lng, :radius]},
               {:primary_key_field => 'id'})

   state :pending
   state :enabled
   state :disabled

   event :enable do
     transitions :from => :pending, :to => :enabled
     transitions :from => :disabled, :to => :enabled
   end

   event :disable do
     transitions :from => :pending, :to => :disabled
     transitions :from => :enabled, :to => :disabled
   end

   before_save :update_cache
   after_save :update_sort

  attr_accessor :google_address
  attr_accessor :postal_code_or_city

  scope :available_for_project, -> { where(:receive_project_messages => true) }
  scope :unavailable_for_project, -> { where(:receive_project_messages => false) }

  def self.search_by_phone_number(phone_numbers)
    search_for_these_phone_numbers = []

    if phone_numbers.is_a?(String)
      search_for_these_phone_numbers << phone_numbers
    elsif phone_numbers.is_a?(Fixnum)
      search_for_these_phone_numbers << phone_numbers.to_s
    elsif phone_numbers.is_a?(Array)
      search_for_these_phone_numbers = phone_numbers.dup
    else
      return []
    end

    phone_numbers_for_sql = []
    search_for_these_phone_numbers.each do |phone_number|
      phone_number_sanitized = phone_number.gsub(/[^0-9a-z]/i, '') # remove all punctuation
      phone_numbers_for_sql << "phones.phone LIKE '%#{phone_number_sanitized}%'"
      phone_numbers_for_sql << "phones.phone LIKE '%1#{phone_number_sanitized}%'" # some will put a 1 at the beginning
    end
    phone_number_sql = "(#{phone_numbers_for_sql.join(' OR ')})"

    # puts "********* phone_number_sql = #{phone_number_sql}"
    service_areas = ServiceArea.find_by_sql("SELECT `service_areas`.* FROM `service_areas` INNER JOIN
                                            `companies` ON `service_areas`.company_id = `companies`.id INNER JOIN
                                            `phones` ON `phones`.company_id = `companies`.id WHERE #{phone_number_sql} and
                                            companies.state in ('approved', 'claimed') and
                                            service_areas.state = 'enabled' ORDER BY companies.`position`")
    return [] unless service_areas.present?

    unique_service_areas = ServiceArea.select("min(service_areas.id) as 'id', service_areas.company_id").where(
      ["id in (#{service_areas.to_s(:db)})"]).group("service_areas.company_id" )

    ServiceArea.find_search_results(ServiceArea.where(
                                    ["(service_areas.id in (#{unique_service_areas.to_a.to_s(:db)}))"]).order(
                                      "FIELD(service_areas.id,#{unique_service_areas.to_a.to_s(:db)})"))
  end

  # will find companies/service areas that match an email address
  # a priority will be placed on a company/service area that match the product provided in :subscribed_to
  # note, matches are not restricted to the product, but a priority is placed on them
  def self.find_using_email(email, options={:subscribed_to => nil})

    company = potential_companies.first
    return company, company.public_service_areas.first
  end

  #ServiceArea.find_using_email(message.from, :subscribed_to => Listing.platinum)

  # 1 - first search service areas - highest priority
  def find_service_area
    potential_service_areas = ServiceArea.where(["users.email = ? and service_areas.state = 'enabled' AND companies.state in ('approved', 'claimed')", email]).includes(:primary_contact, :company).all
    if potential_service_areas.present? && options[:subscribed_to]
      potential_service_areas.each do |service_area|
        company = service_area.company
        if company.listing_subscriptions.current.for_product(options[:subscribed_to]).latest.first
          return company, service_area
        end
      end
    elsif potential_service_areas.present?
      service_area = potential_service_areas.first
      return service_area.company, service_area
    end
  end
  

  def find_find_companies
    # 2 - now search companies
    potential_companies = Company.approved_and_claimed.where(["users.email = ?", email]).includes(:user).all
    return nil unless potential_companies.present?

    if options[:subscribed_to]
      potential_companies.each do |company|
        if company.listing_subscriptions.current.for_product(options[:subscribed_to]).latest.first
          return company, company.public_service_areas.first
        end
      end
    end
  end

   #eagerly load company associations for search results
  def self.find_search_results(query, options={})
   solr_search_results = options.delete(:solr_search_results)
   begin
     service_areas = query.joins(:company).all
     if solr_search_results
       service_areas_enhanced = []
       # add solr_score and geo_distance
       service_areas.each do |service_area|
         solr_record = solr_search_results.detect {|search_result| search_result[0] == service_area.id}
         if solr_record
           class << service_area; attr_accessor :solr_score; attr_accessor :geo_distance; end
           service_area.solr_score = solr_record[1]
           service_area.geo_distance = solr_record[2]
         end
         service_areas_enhanced << service_area
       end
       service_areas = service_areas_enhanced
     end
     service_areas.uniq
   rescue
     return []
   end
  end

  def self.assemble_search_results(search_results)
    service_areas = []
    city_name_facets = category_name_facets = nil

    if search_results
      if search_results.respond_to?(:docs)
        companies = search_results.docs

        if search_results.facets["facet_fields"] && search_results.facets["facet_fields"].size > 0
    		  if search_results.facets["facet_fields"]["solr_category_name_facet"]
      			category_name_facets = search_results.facets["facet_fields"]["solr_category_name_facet"].find_all{|facet| facet.class.name == 'String' && facet =~ /^[A-Za-z]/}
      		end

    		  if search_results.facets["facet_fields"]["solr_city_facet"]
      			city_name_facets = search_results.facets["facet_fields"]["solr_city_facet"] # .find_all{|facet| facet.class.name == 'String' && facet =~ /^[A-Za-z]/}
      		end

  		  end
  		else
        service_areas = search_results
  		end
    end

    [service_areas, city_name_facets, category_name_facets]
  end

  def choose_listing_sponsors(options={:category => nil, :num_sponsors => 3})
    return [] unless options[:category] && self.city && self.city.location

    num_sponsors = options[:num_sponsors] || 3
    category_id = options[:category].id

    # platinum subscribers have double the chance to show up in sponsored results
    # messy implementation as we hard code the product name
    weight_query = <<-QUERY
    CASE products.name
    WHEN "platinum" THEN 2
    else 1
    END as "weight"
    QUERY

    sponsor_service_area_ids = ServiceArea.select("service_areas.id, #{weight_query}, companies.state, companies.id as 'company_id'").
                              joins("inner join companies on service_areas.company_id = companies.id").
                              joins("INNER JOIN `subscriptions` ON subscriptions.company_id = service_areas.company_id AND #{ListingSubscription::PREMIUM_PAID_OR_TRIAL_WHERE_SQL}").
                              joins("inner join cities on service_areas.city_id = cities.id").
                              joins("INNER JOIN locations on cities.id = locations.locatable_id and locatable_type = 'City' and #{Location.distance_sql(self.city.location)} <= #{Homestars::YMAC_RADIUS}").
                              joins("inner join company_categories on company_categories.company_id = subscriptions.company_id and company_categories.enabled = 1 and company_categories.category_id = #{category_id}").
                              joins("inner join products on products.id = subscriptions.product_id AND products.type = 'Listing'").
                              group('service_areas.company_id').
                              where("service_areas.state = 'enabled'").
                              all

    # if no paying sponsors, go get a non-paying
    if sponsor_service_area_ids.empty?
      # platinum subscribers have double the chance to show up in sponsored results
      # messy implementation as we hard code the product name
      weight_query = <<-QUERY
      CASE
      WHEN companies.total_number_of_approved_reviews_cache>0 THEN 2
      else 1
      END as "weight"
      QUERY

      # we may not have closest_city_id or closest_launched_city_id
      # if neither we cannot find a sponsor - so bail
      # minimum is closest_city_id and province_id
      city_sql = ''
      if self.city.location.closest_city_id && self.city.location.province_id
        city_sql = "(service_areas.city_id = #{self.city.location.closest_city_id}"
        if self.city.location.closest_launched_city_id
          city_sql += " OR service_areas.city_id = #{self.city.location.closest_launched_city_id}) AND"
        else
          city_sql += ") AND"
        end
      else
        return []
      end

      sponsor_service_area_ids = ServiceArea.select("service_areas.id, #{weight_query}, companies.state as 'state', companies.id as 'company_id'").
                                joins("inner join companies on companies.id = service_areas.company_id").
                                joins("INNER JOIN locations on locations.locatable_id = service_areas.city_id and locatable_type = 'City' and locations.province_id = #{self.city.location.province_id}").
                                where("#{city_sql} (service_areas.state = 'enabled') AND EXISTS (select 1 from company_categories where company_categories.company_id = service_areas.company_id AND company_categories.category_id = #{category_id} and company_categories.enabled = 1 limit 1) AND companies.total_number_of_approved_reviews_cache > 0").
                                group('service_areas.company_id').all
    end

    return [] if sponsor_service_area_ids.empty?

    # remove non approved/claimed and service_areas belonging to this company
    # we do it here because doing so in mysql was too slow (using slow access method)
    sponsor_service_area_ids.delete_if {|service_area| !['approved', 'claimed'].include?(service_area.state) || service_area.company_id == self.company.id }

    # sponsor_service_area_ids.delete_if {|service_area| !['approved', 'claimed'].include?(service_area.state) }

    service_area_ids = sponsor_service_area_ids.map{|service_area_data| service_area_data.id }.randomize(sponsor_service_area_ids.map{|service_area_data| service_area_data.weight})[0..(num_sponsors-1)]

    sponsor_service_areas = ServiceArea.where(:id => service_area_ids).all
    ServiceArea.make_impressions(sponsor_service_areas, :topic => 'sponsor')

    sponsor_service_areas.randomize
  end

  def self.choose_sponsor_service_areas(options = {:service_areas => nil, :service_area_ids => nil,
                                       :num_sponsors => 3, :cache_key => nil, :cache_minutes => 10, :search_cache_key => nil})

    # platinum subscribers have double the chance to show up in sponsored results
    # messy implementation as we hard code the product name
    weight_query = <<-QUERY
     CASE products.name
     WHEN "platinum" THEN 2
     else 1
     END as "weight"
    QUERY

    service_area_ids = nil
    if options[:search_cache_key] && (service_areas = Tools::is_cached(options[:search_cache_key]))
     service_area_ids = service_areas
    elsif options[:service_areas]
     service_area_ids = options[:service_areas].map {|service_area| service_area.id}
    else
     service_area_ids = options[:service_area_ids]
    end
    return [] unless service_area_ids

    num_sponsors = options[:num_sponsors] || 3
    cache_ttl = options[:cache_minutes] || 10

    if options[:cache_key]
     if options[:force_cache] && Tools::is_cached(options[:cache_key])
       Tools::kill_cached(options[:cache_key])
     end

     sponsor_ids = Tools::cache_me(options[:cache_key], cache_ttl) {
       ServiceArea.select("service_areas.id").joins("INNER JOIN `companies` ON service_areas.company_id = companies.id " +
                          " INNER JOIN `subscriptions` ON subscriptions.company_id = companies.id AND " +
                          " `subscriptions`.`type` = 'ListingSubscription' AND " +
                          " #{ListingSubscription::PREMIUM_PAID_OR_TRIAL_WHERE_SQL} ").where(["service_areas.id in (?)", service_area_ids]).map {|service_area| service_area.id}
      }
      if sponsor_ids.present?
        sponsor_service_area_ids = ServiceArea.select("service_areas.id, #{weight_query}").where("(service_areas.id in (?))", sponsor_ids).joins("inner join companies on companies.id = service_areas.company_id INNER JOIN `subscriptions` ON subscriptions.company_id = companies.id inner join products on products.id = subscriptions.product_id AND products.type = 'Listing' AND #{ListingSubscription::PREMIUM_PAID_OR_TRIAL_WHERE_SQL}").group('service_areas.company_id').all

        selected_ids = sponsor_service_area_ids.map{|service_area_data| service_area_data.id }.randomize(sponsor_service_area_ids.map{|service_area_data| service_area_data.weight})[0..(num_sponsors-1)]

        sponsor_service_areas = ServiceArea.where("(service_areas.id in (?))", selected_ids).all
      else
        sponsor_service_areas = []
      end
    else
      if sponsor_ids.present?
        sponsor_service_area_ids = ServiceArea.select("service_areas.id, #{weight_query}").where("(service_areas.id in (?))", sponsor_ids).joins("inner join companies on companies.id = service_areas.company_id INNER JOIN `subscriptions` ON subscriptions.company_id = companies.id inner join products on products.id = subscriptions.product_id AND products.type = 'Listing' AND #{ListingSubscription::PREMIUM_PAID_OR_TRIAL_WHERE_SQL}").group('service_areas.company_id').all

        selected_ids = sponsor_service_area_ids.map{|service_area_data| service_area_data.id }.randomize(sponsor_service_area_ids.map{|service_area_data| service_area_data.weight})[0..(num_sponsors-1)]

        sponsor_service_areas = ServiceArea.where("(service_areas.id in (?))", selected_ids).all
      else
        sponsor_service_areas = []
      end
    end

    ServiceArea.make_impressions(sponsor_service_areas, :topic => 'sponsor')
    return sponsor_service_areas.randomize
  end

  def solr_position
    if SortOrderSpecification.current && self.city && self.city.location && self.city.location.province && SortOrderSpecification.current.is_for?(self.city.location.province)
      self.position
    else
      company.position
    end
  end

  # OLD Algorithm
  # R + (R-5) * log (N, 1000)
  #
  # "R" is the Rating
  # "N" is the Number of Reviews
  # "5" is the average rating
  # NEW Algorithm
  #
  #Pass array of reviews and it'll give me the reputation rank
  def calculate_sort_position options={}
    sort_order_specification = options[:sort_order_specification]
    reviews_to_consider = options[:specific_reviews_to_consider] || company.approved_reviews # all reviews or just a grouping

    if company.bully?
      self.position = 0
      return self.position
    end

    if sort_order_specification || ((sort_order_specification = SortOrderSpecification.current) && self.city && self.city.location && SortOrderSpecification.current.is_for?(self.city.location.province))

      #     logger.debug("calculating sort position - position = *#{self.position}*")
      # logger.debug("calculate_sort_position caller.inspect == #{caller.inspect}")
      #
      # logger.debug("sort_order_specification.b1 = #{sort_order_specification.b1}")
      # logger.debug("sort_order_specification.n1_lower = #{sort_order_specification.n1_lower}")
      # logger.debug("sort_order_specification.n1_upper = #{sort_order_specification.n1_upper}")
      # logger.debug("sort_order_specification.b2 = #{sort_order_specification.b2}")
      # logger.debug("sort_order_specification.n2_lower = #{sort_order_specification.n2_lower}")
      # logger.debug("sort_order_specification.n2_upper = #{sort_order_specification.n2_upper}")
      # logger.debug("sort_order_specification.alp1 = #{sort_order_specification.alp1}")
      # logger.debug("sort_order_specification.alp2 = #{sort_order_specification.alp2}")
      # logger.debug("sort_order_specification.alp3 = #{sort_order_specification.alp3}")
      # logger.debug("sort_order_specification.alp4 = #{sort_order_specification.alp4}")
      # logger.debug("sort_order_specification.beta = #{sort_order_specification.beta}")
      # logger.debug("sort_order_specification.lamb1 = #{sort_order_specification.lamb1}")
      # logger.debug("sort_order_specification.lamb2 = #{sort_order_specification.lamb2}")
      # logger.debug("sort_order_specification.p = #{sort_order_specification.p}")

      b1 = sort_order_specification.b1
      n1_lower = sort_order_specification.n1_lower
      n1_upper = sort_order_specification.n1_upper
      b2 = sort_order_specification.b2
      n2_lower = sort_order_specification.n2_lower
      n2_upper = sort_order_specification.n2_upper
      alp1 = sort_order_specification.alp1
      alp2 = sort_order_specification.alp2
      alp3 = sort_order_specification.alp3
      alp4 = sort_order_specification.alp4
      beta = sort_order_specification.beta
      lamb1 = sort_order_specification.lamb1
      lamb2 = sort_order_specification.lamb2
      p = sort_order_specification.p

      wsum=0.0
    	ssum=0.0

    	reviews_to_consider.each do |review|
    	  review_writer = review.user

        # logger.debug("user.number_of_approved_reviews = #{review_writer.number_of_approved_reviews}")
        # logger.debug("user.has_uploaded_photos = #{review_writer.has_uploaded_photos}")
        # logger.debug("user.has_an_avatar = #{review_writer.has_an_avatar}")
        # logger.debug("user.participated_in_the_forum = #{review_writer.participated_in_the_forum}")
        # logger.debug("user.valid_email_address = #{review_writer.valid_email_address}")
        # logger.debug("user.any_suspicious_reviews = #{review_writer.any_suspicious_reviews}")
        # logger.debug("review.score = #{review.score}")
        # logger.debug("review.weeks_old = #{review.weeks_old}")
        # logger.debug("company.number_of_approved_reviews = #{company.number_of_approved_reviews}")

        # double tr = log(b1+min(N1,rdata[x][i][3])/n1) * (1 + alp1*rdata[x][i][4] + alp2*rdata[x][i][5] + alp3*rdata[x][i][6] + alp4*rdata[x][i][7] - beta * rdata[x][i][8]) * rdata[x][i][2];
        tr = Math.log(b1 + [n1_upper,(review_writer.number_of_approved_reviews/n1_lower)].min) *
             (1 + alp1*review_writer.has_uploaded_photos +
              alp2*review_writer.has_an_avatar +
              alp3*review_writer.participated_in_the_forum +
              alp4*review_writer.valid_email_address -
              beta*review_writer.any_suspicious_reviews) * review.quality(sort_order_specification)

        # double w = tr * exp(-1*lamb1*rdata[x][i][0]);
        w = tr * Math.exp(-1 * lamb1 * review.weeks_old)

    	  wsum += w

    	  # ssum += w * pow( (rdata[x][i][1]-5) * exp(-1*lamb2*rdata[x][i][0]) , p);
    	  ssum += w * (((review.score-5) * Math.exp(-1 * lamb2 * review.weeks_old)) ** p)
    	end
    	# double result = log(b2 + (min(N2,company[x][1]) /n2)) * pow(ssum/wsum, 1/p);

      if wsum > 0
      	self.position = Math.log(b2 + [n2_upper, reviews_to_consider.size].min/n2_lower) * (ssum/wsum)**(1/p)
      	self.position = ( (self.position + 7.0) / 2.0 )
      	self.position = self.position * [1.23, reviews_to_consider.size].min
      	self.position = self.position * 16.26 # scale the max position from 0.200 (1.23*16.26=20) - original scale was 20
        # * 20 # *20 gives us a range of 0..200
      else
        self.position = 0
      end
      # logger.debug("position = #{self.position}")
    	# the following line normalizes the result to a value between 0 and 10
    	#  * 5 would give us a number between 0 and 50
    # logger.debug("DONE calculating sort position - position = *#{self.position}*")
    else

      if company.avg_rating_cache && company.total_number_of_approved_reviews_cache && company.total_number_of_approved_reviews_cache > 0
        self.position = company.avg_rating_cache + ((company.avg_rating_cache-5) * Math.log(company.total_number_of_approved_reviews_cache, 1000))
      else
        self.position = 1
      end
    end

    # don't set the overall position if we are considering only a subsection of reviews
    unless options[:specific_reviews_to_consider].present?
      self.update_column(:position, self.position) #skip callsbacks
    end

    self.position
  end


  # middle of the atlantic - that way we get a blue map of nothing
  def default?
    self.longitude == -54.667969 && self.latitude == 29.242951
  end
  def default
    self.longitude = -54.667969
    self.latitude = 29.242951
  end

  def location
    published_location ? published_location : company.location
  end

  def country
    city.location.country.iso
  end

  def latitude=(value)
    @latitude = value
  end
  def latitude
    lat
  end
  def lat
    city ? city.location.latitude : @latitude
  end

  def longitude=(value)
    @longitude = value
  end
  def longitude
    lng
  end
  def long
    lng
  end
  def lng
    city ? city.location.longitude : @longitude
  end

  def radius
    city.location.radius
  end

  def city_name
    city.name
  end

  # solr indexed fields
  def categories
    company.public_categories
  end

  def solr_category_names
    company.public_categories.map {|cat| cat.name}
  end

  def solr_company_id
    company.id
  end

  def solr_avg_rating
    company.avg_rating
  end

  def solr_first_letter_of_name
    company.first_letter_of_name
  end

  def solr_number_of_photos
    company.number_of_photos
  end

  def solr_number_of_reviews
    company.number_of_reviews
  end

  def solr_listing
    company.listing
  end

  def solr_company_state
    company.state
  end

  def solr_state
    state
  end

  def solr_city
    city_name
  end

  def solr_category_name
    company.category_name
  end

  def solr_profile
    company.profile_search
  end

  def solr_categories
    company.categories_info
  end

  #solr needs this name
  def company_name
    sort_name
  end

  def sort_name
    company.sort_name
  end

  def solr_country
    country
  end

  # from company
  def name
    closest_city_cache
  end

  def profile
    company.profile_to_display
  end

  # use the service area's position if we have a pertaining sort_order_specification
  # else use the old company level position
  def boost_me
    if city.location.province && SortOrderSpecification.current && SortOrderSpecification.current.is_for?(city.location.province)
      return self.position
    else
      return company.boost_me
    end
  end

  # don't look at primary contact, as Users don't have phone numbers
  def contact_phone_number
    if location && !location.phone.blank?
      location.phone
    else
      company.contact_phone_number
    end
  end

  def contact_email_address
    if location && !location.email.blank?
      location.email
    elsif primary_contact && !primary_contact.email.blank?
      primary_contact.email
    else
      company.contact_email_address
    end
  end

  def contact_name
    self.contact.try(:full_name) || self.contact.try(:login) || ""
  end

  def contact
    primary_contact || company.user
  end

  # nohup script/runner -e staging "ServiceArea.export(:filename => '/var/www/apps/homestars/service_areas.xml')"
  # GTA ServiceArea.export(:conditions => 'city_id in (1, 90, 76, 107, 89, 64, 70, 85, 109, 102, 146, 73, 518, 79)')
  def self.export(options={})
    filename = options.delete(:filename)
    filename ||= '/tmp/service_areas.xml'
    rebuild_category_cache = options.delete(:rebuild_category_cache)
    rebuild_entire_cache = options.delete(:rebuild_entire_cache)

    x = '<?xml version="1.0" encoding="UTF-8"?><service-areas>'
    File.open("#{filename}", "w") do |file|
      file.puts x
      export = Builder::XmlMarkup.new(:target => file, :indent => 1)

      ServiceArea.find_each(options) do |service_area|
        begin
          if rebuild_entire_cache
            service_area.company.rebuild_cache
          elsif rebuild_category_cache
            service_area.company.rebuild_category_cache
          end

          file.write service_area.to_xml(:skip_instruct => true)
          file.flush
        rescue Exception => unknown
           message = unknown.message
           backtrace = "\nBacktrace:\n#{unknown.backtrace.join("\n")}"
           logger.error("Error exporting service_area to XML exception class = #{unknown.class}")
           logger.error("Error exporting service_area to XML ; msg+backtrace: #{message}+#{backtrace}")
           logger.error("Error exporting service_area to XML ; service_area.inspect: #{service_area.inspect}")
         end
      end

      file.puts '</service-areas>'
    end
  end

  def to_xml(options = {})
   options[:indent] ||= 2
   options[:dasherize] ||= false
   options[:skip_instruct] ||= true
   xml = options[:builder] ||= Builder::XmlMarkup.new(:indent => options[:indent])
   xml.instruct! unless options[:skip_instruct]
   xml.service_area do
     xml.tag!(:id, id)

     xml.tag!(:type, 'ServiceArea')

     xml.tag!(:company_id, company_id)
     xml.tag!(:name, name)

     xml.tag!(:sort_name, sort_name)
     xml.tag!(:company_name, company_name)
     xml.tag!(:solr_categories, solr_categories)
     xml.tag!(:solr_profile, solr_profile)
     xml.tag!(:solr_position, solr_position)
     xml.tag!(:solr_first_letter_of_name, solr_first_letter_of_name)
     xml.tag!(:solr_country, solr_country)
     xml.tag!(:solr_avg_rating, solr_avg_rating)
     xml.tag!(:solr_number_of_photos, solr_number_of_photos)
     xml.tag!(:solr_number_of_reviews, solr_number_of_reviews)
     xml.tag!(:solr_listing, solr_listing)
     xml.tag!(:solr_city, solr_city)
     xml.tag!(:solr_company_state, solr_company_state)
     xml.tag!(:solr_state, solr_state)
     xml.tag!(:boost, boost_me)

     xml.tag!(:lng, lng)
     xml.tag!(:lat, lat)
     xml.tag!(:radius, radius)

     xml.categories do
      categories.each do |category|
        xml.tag!(:id, category.id)
      end
     end
    end
  end

private

  def record_ruby_call_stack
    self.ruby_call_stack = caller(0)
  end

  def set_created_by
    if (context = RequestContext.current) && context.user
      self.created_by =  context.user.id
    end
  end

  def set_updated_by
    if (context = RequestContext.current) && context.user
      self.updated_by =  context.user.id
    end
  end

  def update_sort
    self.delay(:queue => 'solr').calculate_sort_position
  end

  def update_cache
    c = City.find(self.city_id)
    self.closest_city_cache = c.name
    self.description = c.name
  end

end
